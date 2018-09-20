# this script will not be used.


import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
from sets import Set
import boto3
import redis
import pickle
import numpy as np
import random
import json


# Extract relevant data from json body
def extract_data(json_body):

    json_body = json.loads(json_body)

    try:
        # Sender data
        from_id = json_body['actor']['id']
        from_firstname = json_body['actor']['firstname']
        from_lastname = json_body['actor']['lastname']
        from_username = json_body['actor']['username']
        is_business = json_body['actor']['is_business']

        # Receiver data
        to_id = json_body['transactions'][0]['target']['id']
        to_firstname = json_body['transactions'][0]['target']['firstname']
        to_lastname = json_body['transactions'][0]['target']['lastname']
        to_username = json_body['transactions'][0]['target']['username']
        is_business = is_business or json_body['transactions'][0]['target']['is_business']

        timestamp = json_body['created_time']
    except:
        return None

    amount=random.randint(1,500)

    # Output data dictionary
    data = {'from_id': int(from_id),
            'to_id': int(to_id),
            'timestamp': timestamp
            'transaction_amount' : amount}
    return data

# Analysis
def analysis()

    infile=open(filename,'rb')
    db=pickle.load(infile)


    dbkey=set()

    for t in data:

        if t['from_id'] in dbkey:
            if data['transaction_amount']>db['from_id']['max_sending']:
                db['from_id']['max_sending']=data['transaction_amount']
            elif data['transaction_amount']<db['from_id']['min_sending']:
                db['from_id']['min_sending'] = data['transaction_amount']
        elif t['from_id'] not in dbkey:
            dbkey.add('from_id')
            db['from_id']['max_sending']=data['transaction_amount']
            db['from_id']['min_sending'] = data['transaction_amount']



        if t['to_id'] in dbkey:
            if data['transaction_amount']>db['to_id']['max_receiving']:
                db['to_id']['max_receiving']=data['transaction_amount']
            elif data['transaction_amount']<db['to_id']['min_receiving']:
                db['to_id']['min_receiving'] = data['transaction_amount']
        elif t['to_id'] not in dbkey:
            dbkey.add('from_id')
            db['to_id']['max_receiving']=data['transaction_amount']
            db['to_id']['min_receiving'] = data['transaction_amount']




"""
# Send data to Redis databases
def send_partition(iter_data):

    # Redis connection
    redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com' # Set Redis connection (local)
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    pickled_iter_data = pickle.dumps(iter_data)
    redis_db.set('iter_data_key', pickled_iter_data)
"""

# To Run:
# sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 kafka-spark-test.py
if __name__ == "__main__":


    # Set up resources
    sc = SparkContext(appName="Venmo-User-Analytics")
    ssc = StreamingContext(sc, 1)  # Set Spark Streaming context
    ssc.checkpoint("checkpoint")

    # brokers = "ec2-50-112-19-115.us-west-2.compute.amazonaws.com:9092,ec2-52-33-162-7.us-west-2.compute.amazonaws.com:9092,ec2-52-89-43-209.us-west-2.compute.amazonaws.com:9092"
    brokers = "ec2-52-25-139-222.us-west-2.compute.amazonaws.com:9092"
    topic = 'Venmo-Transactions-Dev'

    kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    transaction = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))

    running_counts = transaction.map(lambda data: ('count', 1)).updateStateByKey(update_func)
#        .foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    # transaction.pprint()

    ssc.start()
    ssc.awaitTermination()