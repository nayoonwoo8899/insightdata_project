#!/bin/sh

$SPARK_HOME/bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar --master spark://ec2-107-23-227-201.compute-1.amazonaws.com:7077 --executor-memory 6G spark_streaming.py
