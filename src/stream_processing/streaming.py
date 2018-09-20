import time
import json
import random
import redis
from kafka import KafkaConsumer

class Streaming(threading.Thread):
    daemon = True

    # Constructor sets up Redis connection and algorithm vars
    def __init__(self):
        super(Streaming, self).__init__()

        # Set up connection to Redis server
        self.redis_server = 'localhost'
        self.redis_db = redis.StrictRedis(host=self.redis_server, port=6379, db=0)

        # intiailize internal variables

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        consumer.subscribe(['venmo-transactions'])

        for message in consumer:
            msg = str(message.value)
            __analyze_message__(msg)

    # Assign colors to message based on emoji/text content
    def __analyze_message__(self, json_obj):
        json_data = json.loads(json_obj)

        # get user id
        from_id = json_data['actor']['id']
        to_id = json_data['transactions'][0]['target']['id']

        # read previous value from Redis
        prev_send_max = self.redis_db.get(from_id+'maxx')
        prev_send_min = self.redis_db.get(from_id+'minn')
        prev_receive_max = self.redis_db.get(to_id+'maxx')
        prev_receive_min = self.redis_db.get(to_id+'minn')

        # generate fake transaction amount
        amount = random.randint(1, 500)

        # get new max / min and update Redis db
        if amount>prev_send_max or prev_send_max==None:
            self.redis_db.set(from_id+'maxx',amount)
        if amount<prev_send_min or prev_send_min==None:
            self.redis_db.set(from_id+'minn',amount)
        if amount>prev_receive_max or prev_receive_max==None:
            self.redis_db.set(to_id+'maxx',amount)
        if amount<prev_receive_min or prev_receive_min==None:
            self.redis_db.set(to_id+'minn',amount)




if __name__ == "__main__":

    thread = Streaming()

    while True:
        if not thread.isAlive():
            print("Starting Kafka consumer...")
            thread.start()
            print("Started Kafka consumer.")
        else:
            print("Listening for new messages in topic: 'venmo-transactions'...")
time.sleep(15)