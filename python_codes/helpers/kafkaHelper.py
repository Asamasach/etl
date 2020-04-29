from confluent_kafka import Consumer
from elasticHelper import Elastic
from logHelper import Log
from mysqlHelper import Mysql
import yaml
import os
import time

fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/kafka.yml")

class Kafka():

    def __init__(self, topic_name, group_id, auto_offset_reset, kafka_id):
        
        with open(config_file_path) as kafka_conf:
           self.conf = yaml.load(kafka_conf, Loader=yaml.FullLoader)
        self.mysql = Mysql()
        self.elasticsearch_instance = Elastic()
        self.group_id = group_id
        self.topic_name = topic_name
        self.auto_offset_reset = auto_offset_reset
        self.running_consumer = True
        self.kafka_id = kafka_id
        self.c = Consumer({
                    'bootstrap.servers': self.conf['bootstrap_servers'],
                    'group.id': self.group_id,
                    'auto.offset.reset': self.auto_offset_reset
                    })
        self.c.subscribe([self.topic_name])
        print(self.c.list_topics())
        print("{}th kafka_object has created!".format(self.kafka_id))
    def consume(self, index, consumer_id):
#        self.batch_size = batch_size
        a = 0
        data = []
        self.index = index
        self.consumer_id = consumer_id
        self.old_consumer_record = self.mysql.get_list(consumer_id = self.consumer_id)
        while self.running_consumer:
            
            msg = self.c.poll(1.0)

            if msg is None:
                a+=1

                print("empty message!")

            else:
                a+=1
                msg = msg.value().decode('utf-8')
                data.append(msg)
                       
            if a % 10 == 0:

                self.running_consumer = False
                if len(data) > 5:
                    self.elasticsearch_instance.post(data= data, index= self.index)
                    print("elk_consume for index : {}".format(self.index))
                # check change in mysql
                data = []
                a = 0
                consumer_record = self.mysql.get_list(consumer_id = self.consumer_id)
                if consumer_record != self.old_consumer_record:
                    self.old_consumer_record = consumer_record
                    print("record has changed in database!")
                    self.c.close()
                    break
                else:
                    self.c.commit()
                    self.running_consumer = True

        return None
