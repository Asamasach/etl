from confluent_kafka import Consumer
from helpers.logHelper import Log
import yaml

class Kafka():

    def __init__(self, topic_name, group_id, auto_offset_reset):
        
        with open(r'../config/kafka.yml') as kafka_conf:
            self.conf = yaml.load(kafka_conf)
        # self.consumer_id = consumer_id
        self.group_id = group_id
        self.topic_name = topic_name
        self.auto_offset_reset = auto_offset_reset

        self.c = Consumer({
                    'bootstrap.servers': self.conf['bootstrap_servers'],
                    'group.id': self.group_id,
                    'auto.offset.reset': auto_offset_reset
                    })
        self.c.subscribe([self.conf['topic_name']])
    
    def consume(self):

        msg = self.c.poll(1.0)

        if msg is None:
            empty = Log("Empty")
            empty.write("Empty message!","kafka")
        if msg.error():
            err = Log("Error")
            err.write(msg.error(),"kafka")
        self.c.close()
        print(msg.value().decode('utf-8'))
        
        return msg