#!/bin/sh

$SPARK_HOME/bin/spark-submit --master spark://ec2-107-23-227-201.compute-1.amazonaws.com:7077 --executor-memory 6G spark_batch2.py
