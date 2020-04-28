from confluent_kafka import Consumer
from logHelper import Log
import yaml
import os

fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/kafka.yml")

class Kafka():

    def __init__(self, topic_name, group_id, auto_offset_reset):
        
        with open(config_file_path) as kafka_conf:
           self.conf = yaml.load(kafka_conf, Loader=yaml.FullLoader)

        self.group_id = group_id
        self.topic_name = topic_name
        self.auto_offset_reset = auto_offset_reset
        self.running_consumer = True

        self.c = Consumer({
                    'bootstrap.servers': self.conf['bootstrap_servers'],
                    'group.id': self.group_id,
                    'auto.offset.reset': self.auto_offset_reset
                    })
        self.c.subscribe([self.topic_name])
    
    def consume(self):
        while self.running_consumer:
            msg = self.c.poll(1.0)

            if msg is None:
                empty = Log("Empty")
                empty.write("Empty message!","kafka")

        #    if msg.error():
        #        err = Log("Error")
        #        err.write(msg.error(),"kafka")

        #    print(msg.value().decode('utf-8'))
            self.c.commit()
        self.c.close()
        return msg
    
    def stop_consume(self):
        self.running_consumer = False
