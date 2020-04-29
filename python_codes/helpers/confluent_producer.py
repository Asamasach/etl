from confluent_kafka import Producer
from faker import Faker
from random import randint
import time
fake = Faker('en_US')

p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

some_data_source = { "new": "1" ,"newer" : 2 }

if __name__ == "__main__":
    while True:
        my_dict = {  'first_name': fake.first_name(), 'last_name': fake.last_name(), 'age': randint(0, 100)   } 
        print(str(my_dict))
        print("\n")

    #for data in my_dict:
    # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
        p.produce('asamasach_3', str(my_dict).encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
        p.flush()
        time.sleep(1)
