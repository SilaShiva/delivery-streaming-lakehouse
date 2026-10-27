"""Real-time tier (DataStream walkthrough): 10-minute event-time windows.

Demonstrates the streaming-interview core:
  * event-time windows + watermarks (out-of-order data handled correctly)
  * late events beyond the watermark -> side output (audited, not dropped, per D4)
  * windowed GMV / order counts emitted to Kafka "live-metrics" topic

This file is the DataStream-API walkthrough — the version of the job interviewers
will want to talk through line by line. The job that actually runs on this
Windows-native stack is the Flink SQL one (flink/sql/realtime_metrics.sql),
submitted via flink/iceberg_sink/submit_sql_windows.py, because PyFlink's
DataStream API requires a Linux host upstream (docs/decisions.md D2/D11).

Submit (if you have a Linux box or WSL for it):
    flink run -py flink/pyflink_walkthrough/realtime_metrics.py
"""

from __future__ import annotations

import json

from pyflink.common import Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer,
                                                 KafkaRecordSerializationSchema,
                                                 KafkaSink, KafkaSource)
from pyflink.datastream.datastream import OutputTag
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import Time, TumblingEventTimeWindows

KAFKA = "localhost:9092"
WATERMARK_DELAY_MIN = 5          # D4
WINDOW_MIN = 10

LATE_TAG = OutputTag("late-orders", Types.STRING())


class GmvAggregate(AggregateFunction):
    """(gmv, created, delivered, cancelled) accumulated per restaurant-window."""

    def create_accumulator(self):
        return (0.0, 0, 0, 0)

    def add(self, event: dict, acc: tuple) -> tuple:
        gmv, created, delivered, cancelled = acc
        et = event["event_type"]
        if et == "created":
            created += 1
        elif et == "delivered":
            delivered += 1
            gmv += event["order_value"]
        elif et == "cancelled":
            cancelled += 1
        return (gmv, created, delivered, cancelled)

    def get_result(self, acc: tuple) -> dict:
        gmv, created, delivered, cancelled = acc
        return {"gmv": round(gmv, 2), "created": created,
                "delivered": delivered, "cancelled": cancelled}

    def merge(self, accs):
        return (sum(a[0] for a in accs), sum(a[1] for a in accs),
                sum(a[2] for a in accs), sum(a[3] for a in accs))


class StampWindow(ProcessWindowFunction):
    """Attach key + window bounds to the aggregate result."""

    def process(self, key, context, elements):
        (agg,) = elements
        agg["restaurant_id"] = key
        agg["window_start"] = context.window().start
        agg["window_end"] = context.window().end
        yield json.dumps(agg)


def run() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30_000)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA) \
        .set_topics("orders-json") \
        .set_group_id("flink-realtime") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    watermarks = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_DELAY_MIN)) \
        .with_timestamp_assigner(lambda ev, ts: json.loads(ev)["event_ts"])

    events = env.from_source(source, watermarks, "orders-json", type_info=Types.STRING())

    parsed = events.map(lambda ev: json.loads(ev),
                        output_type=Types.PICKLED_BYTE_ARRAY()) \
                   .filter(lambda e: e["event_type"] in ("created", "delivered", "cancelled"),
                           output_type=Types.PICKLED_BYTE_ARRAY())

    windowed = (
        parsed
        .key_by(lambda e: e["restaurant_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_MIN)))
        .side_output_late_data(LATE_TAG)
        .aggregate(GmvAggregate(), StampWindow())
        .name("windowed-gmv")
    )

    # D4: late-beyond-watermark events are audited to a dead-letter topic, not dropped
    late_stream = windowed.get_side_output(LATE_TAG)
    late_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic("late-orders")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()) \
        .build()
    late_stream.sink_to(late_sink).name("late-dead-letter")

    metrics_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic("live-metrics")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()) \
        .build()

    windowed.sink_to(metrics_sink).name("live-metrics-sink")

    env.execute("realtime_metrics")


if __name__ == "__main__":
    run()
