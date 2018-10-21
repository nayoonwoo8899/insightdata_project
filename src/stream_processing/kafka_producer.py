"""
Python script for replaying Venmo data from Amazon S3 to simulate stream processing.
Usage:
    kafka_producer.py [replay date]
Author: Nayoon Woo (nayoonwoo8899@gmail.com)
"""

import sys
import threading
import time
from kafka import KafkaProducer
import smart_open


class Producer(threading.Thread):
    daemon = True
    
    def __init__(self, date):
        self.year = date[0:4]
        self.month = date[4:6]
        self.day = date[6:8]

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        bucket_name = 'venmo-json'
        bucket = self.__get_s3_bucket__(bucket_name)
        
        # read and buffer data from S3
        buffer = []
        for line in smart_open.smart_open("s3://venmo-json/" + self.year + "_" + self.month + "/venmo_" + self.year + "_" + self.month + "_" + self.day +".json"):
            buffer.append(line)

        # Send data from buffer to Kafka queue
        for json_obj in reversed(buffer):
            producer.send('venmo-transactions', json_obj)
            time.sleep(0.001)
            print(json_obj + '\n' + '=================================================================' + '\n')
        
def main():
    
    # read arguments
    args = sys.argv
    print(args)
    if len(args) < 2:
        print(__doc__)
        sys.exit(1)
        
    # parse files
    replay_date = args[1]
    
    # process data
    producer = Producer(replay_date)
    producer.start()
    while True:
        time.sleep(10)
    
        
if __name__ == '__main__':
    main()
