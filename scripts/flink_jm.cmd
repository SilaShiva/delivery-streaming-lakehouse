@echo off
set JAVA=C:\Users\sila.shivanandan\Desktop\Swiggy\DS Projects\delivery-streaming-lakehouse\runtimes\zulu17.56.15-ca-jdk17.0.14-win_x64\bin\java.exe
set FLINK=C:\Users\sila.shivanandan\Desktop\Swiggy\DS Projects\delivery-streaming-lakehouse\runtimes\flink-2.2.1
set LOGS=C:\Users\sila.shivanandan\Desktop\Swiggy\DS Projects\delivery-streaming-lakehouse\.state\logs
set LOG4J=file:///C:/Users/sila.shivanandan/Desktop/Swiggy/DS^ Projects/delivery-streaming-lakehouse/runtimes/flink-2.2.1/conf/log4j.properties
"%JAVA%" -Xmx1024m "-Dlog.file=%LOGS%\flink-jobmanager.log" "-Dlog4j.configurationFile=%FLINK%\conf\log4j.properties" -cp "%FLINK%\lib\*" org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint --configDir "%FLINK%\conf"
