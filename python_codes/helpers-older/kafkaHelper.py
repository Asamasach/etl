from confluent_kafka import Consumer
from logHelper import Log
import yaml
import os
import time

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
        print(self.c.list_topics())
 
    def consume(self):
#        self.batch_size = batch_size
        while self.running_consumer:
            a = 0
            msg = self.c.poll(1.0)

            if msg is None:
               
        #        empty = Log("Empty")
        #        empty.write("Empty message!","kafka")
                print("empty message!")
                msg = "empty".encode('utf-8')
                #if a%10 == 0:
                #break                
                
        #    if msg.error():
        #        err = Log("Error")
        #        err.write(msg.error(),"kafka")

        #    print(msg.value().decode('utf-8'))
            else:
                a+=1
                msg = msg.value().decode('utf-8')
            print("message is : {}".format(msg))#.decode('utf-8')))
            self.c.commit()
            if a % 10 == 0:
                self.running_consumer = False
            #return msg
#        self.c.close()
        return msg
        self.consume()

    def stop_consume(self):
        self.running_consumer = False
        time.sleep(10)
        self.consume()
