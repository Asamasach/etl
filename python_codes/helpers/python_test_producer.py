from kafka import KafkaProducer #for producing data
from kafka.errors import KafkaError
import json #to change dict to json
from faker import Faker #to create random arbitary names
import random #to create random age
from random import randint

import logging

fake = Faker('en_US')

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii')) #set kafkaProducer for json
for _ in range(10):
    my_dict = {  'first_name': fake.first_name(), 'last_name': fake.last_name(), 'age': randint(0, 100)   } 
    print(str(my_dict))
    print("\n")
    future = producer.send('asamasach_3', my_dict) #send json data to kafka topic named 'asamasach_3'
try:
    record_metadata = future.get(timeout=10) #to chech metadata of sending
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)
