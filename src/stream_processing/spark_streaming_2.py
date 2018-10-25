"""
Spark Python script for analyzing Kafka stream and writing to Redis.
Usage:
    $SPARK_HOME/bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.2.jar --master spark://ec2-107-23-227-201.compute-1.amazonaws.com:7077 --executor-memory 6G spark_streaming.py
Author: Nayoon Woo (nayoonwoo8899@gmail.com)
"""

import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row

from pyspark.sql.functions import unix_timestamp
import time
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import *
import datetime
import redis

redis_server = 'ec2-54-82-188-230.compute-1.amazonaws.com'

def process(rdd):
    if rdd.isEmpty():
        return

    rdd.foreachPartition(processPartition)

def processPartition(partition):
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)
    for x in partition:
        json_data = json.loads(x)
        timestamp = json_data['created_time']
        year = int(timestamp[0:4])
        month = int(timestamp[5:7])
        date = int(timestamp[8:10])
        hour = int(timestamp[11:13])
        minn = int(timestamp[14:16])
        secc = int(timestamp[17:19])
        dayofweek = int(datetime.datetime(year, month, date).strftime('%w'))

        # read previous value from Redis
        #hour_string = redis_db.get('hour')
        minn_string = redis_db.get('minn')
        count_string = redis_db.get('counting')
        prev_minn = None
        if minn_string != None:
            prev_minn=int(minn_string)
        counting = None
        if count_string != None:
            counting=int(count_string)

        #if prev_hour == None or prev_hour != hour:
        if prev_minn == None or prev_minn != minn:
            redis_db.set('year', year)
            redis_db.set('month', month)
            redis_db.set('date', date)
            redis_db.set('day of week', dayofweek)
            redis_db.set('hour', hour)
            redis_db.set('min', minn)
            redis_db.set('sec', secc)
            redis_db.set('counting', 1)

        else:

            redis_db.set('year', year)
            redis_db.set('month', month)
            redis_db.set('date', date)
            redis_db.set('day of week', dayofweek)
            redis_db.set('hour', hour)
            redis_db.set('min', minn)
            redis_db.set('sec', secc)
            redis_db.set('counting', counting + 1)


if __name__ == "__main__":
    sc = SparkContext(appName="venmo-transactions-app")
    #sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,1)
    topic_name = "venmo-transactions"
    brokers_dns_str = "ec2-54-82-188-230.compute-1.amazonaws.com:9092"

    streamFromKafka = KafkaUtils.createDirectStream(ssc, [topic_name],{"metadata.broker.list":brokers_dns_str})
    lines = streamFromKafka.map(lambda x: x[1]).map(lambda x : str(x))

    lines.count().pprint()
    lines.foreachRDD(process)
    #text_counts = lines.map(lambda tweet: (tweet['hashtag'],1)).reduceByKey(lambda x,y: x + y)
    ssc.start()
    ssc.awaitTermination()
