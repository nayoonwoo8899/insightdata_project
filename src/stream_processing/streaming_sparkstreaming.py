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
from datetime import datetime
import redis

redis_server = 'ec2-54-82-188-230.compute-1.amazonaws.com'
redis_db = redis.StrictRedis(host=self.redis_server, port=6379, db=0)



"""
artist_list = set()
with open('../data/lower-artist.csv', 'r') as f:
    count =1
    for i in f:
        cur = i.split("\n")[0]
        if count in range(100):
            artist_list.add(cur)
        count+=1

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


@udf('string')

def func(text):
    for word in artist_list:
        if word in text.lower():
            return word

@udf('string')

def transfer_time(text):
    #return "2018-06-25"
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")
"""


def process(json_obj):
    json_data = json.loads(json_obj)

    timestamp = json_data['created_time']
    year = int(timestamp[0:4])
    month = int(timestamp[5:7])
    date = int(timestamp[8:10])
    hour = int(timestamp[11:13])
    dayofweek = int(datetime.datetime(year, month, date).strftime('%w'))

    # read previous value from Redis
    prev_hour = redis_db.get('hour')
    counting = redis_db.get('counting')

    if prev_hour == None or prev_hour != hour:
        print('year=', year, 'month=', month, 'date=', date, 'day of week=', dayofweek, 'hour=', hour,
              '# of transaction during past 1 hour=', counting)
        redis_db.set('year', year)
        redis_db.set('month', month)
        redis_db.set('date', date)
        redis_db.set('day of week', dayofweek)
        redis_db.set('hour', hour)
        redis_db.set('counting', 1)

    else:

        redis_db.set('year', year)
        redis_db.set('month', month)
        redis_db.set('date', date)
        redis_db.set('day of week', dayofweek)
        redis_db.set('hour', hour)
        redis_db.set('counting', counting + 1)

"""
def process(rdd):
    spark = getSparkSessionInstance(rdd.context.getConf())
    venmoDataFrame = spark.read.json(rdd)
    df = venmoDataFrame.withColumn('hashtag', func(tweetsDataFrame.text))
    df = df.withColumn('time',transfer_time(tweetsDataFrame.time))
    df.createOrReplaceTempView("historicaltweets")
    df = spark.sql("SELECT MAX(time) AS time,hashtag, count(*) AS count FROM historicaltweets WHERE hashtag IS NOT NULL GROUP BY hashtag ORDER BY count DESC")
    rdd = df.rdd.map(tuple)
    rdd.saveToCassandra("twitter","tweet")
    df.show()
"""


if __name__ == "__main__":
    sc = SparkContext(appName="venmo-transactions-app")
    #sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,1)
    topic_name = "venmo-transactions"
    brokers_dns_str = "0.0.0.0:9092"

    streamFromKafka = KafkaUtils.createDirectStream(ssc, [topic_name],{"metadata.broker.list":brokers_dns_str})
    lines = streamFromKafka.map(lambda x: x[1])

    lines.count().pprint()
    lines.foreachRDD(process)
    #text_counts = lines.map(lambda tweet: (tweet['hashtag'],1)).reduceByKey(lambda x,y: x + y)
    ssc.start()
    ssc.awaitTermination()
