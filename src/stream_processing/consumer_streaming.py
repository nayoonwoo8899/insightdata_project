"""
Python script for analyzing Kafka stream and writing to Redis.
Usage:
    consumer_streaming.py
Author: Nayoon Woo (nayoonwoo8899@gmail.com)
"""

import threading
import time
import json
import redis
from kafka import KafkaConsumer
import datetime



class Streaming(threading.Thread):
    daemon = True

    # Constructor sets up Redis connection and algorithm vars
    def __init__(self):
        super(Streaming, self).__init__()

        # Set up connection to Redis server
        self.redis_server = 'ec2-54-82-188-230.compute-1.amazonaws.com'
        self.redis_db = redis.StrictRedis(host=self.redis_server, port=6379, db=0)

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='ec2-54-82-188-230.compute-1.amazonaws.com:9092')
        consumer.subscribe(['venmo-transactions'])

        for message in consumer:
            msg = str(message.value)
            self.analyze_message(msg)

    # Assign colors to message based on emoji/text content
    def analyze_message(self, json_obj):
        
        json_data = json.loads(json_obj)
        timestamp = json_data['created_time']
        year=int(timestamp[0:4])
        month = int(timestamp[5:7])
        date=int(timestamp[8:10])
        hour=int(timestamp[11:13])
        minn=int(timestamp[14:16])
        secc=int(timestamp[17:19])
        dayofweek=int(datetime.datetime(year, month, date).strftime('%w'))        

        # read previous value from Redis        
        prev_hour=int(self.redis_db.get('hour'))
        counting=int(self.redis_db.get('counting'))


        if prev_hour==None or prev_hour!=hour:
            self.redis_db.set('year',year)
            self.redis_db.set('month',month)
            self.redis_db.set('date',date)
            self.redis_db.set('day of week',dayofweek)
            self.redis_db.set('hour',hour)
            self.redis_db.set('min',minn)
            self.redis_db.set('sec',secc)
            self.redis_db.set('counting',1)
        
        else:
            self.redis_db.set('year',year)
            self.redis_db.set('month',month)
            self.redis_db.set('date',date)
            self.redis_db.set('day of week',dayofweek)
            self.redis_db.set('hour',hour)
            self.redis_db.set('min',minn)
            self.redis_db.set('sec',secc)
            self.redis_db.set('counting',counting+1)
            
       
                 
if __name__ == "__main__":
    thread = Streaming()
    
    while True:
        if not thread.isAlive():
            print("Starting Kafka consumer...")
            thread.start()
            print("Started Kafka consumer.")
        else:
            print("Listening for new messages in topic: 'venmo-transactions'...")
        time.sleep(10)

