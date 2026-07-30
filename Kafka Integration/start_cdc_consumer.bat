@echo off
REM Start CDC Consumer with proper environment variables
REM This batch file ensures HADOOP_HOME is set before Spark starts

REM Set HADOOP_HOME
set HADOOP_HOME=%USERPROFILE%\.hadoop_home

REM Set Python paths (avoid python3 warning)
set PYSPARK_DRIVER_PYTHON=python
set PYSPARK_PYTHON=python

REM Log startup
echo [%date% %time%] Starting CDC Consumer... >> cdc_startup.log
echo HADOOP_HOME=%HADOOP_HOME% >> cdc_startup.log

REM Start CDC consumer in background
start "SyniqAI CDC Consumer" /B python spark_cdc_consumer.py --checkpoint "C:/temp/spark-checkpoints" >> cdc_stdout.log 2>> cdc_stderr.log

REM Log PID (not available in batch, will be saved by Python script)
echo [%date% %time%] CDC Consumer started >> cdc_startup.log
