from kafkaHelper import Kafka
from mysqlHelper import Mysql
from elasticHelper import Elastic
import multiprocessing
import time

class Consumer():

    def __init__(self, consumer_group_id):
        self.consumer_group_id = consumer_group_id

        self.old_consumers = []
        self.p = []
        self.elasticsearch_instance = Elastic()

    def consume(self):
        consumers = []
        mysql = Mysql()
        consumers = mysql.get_list(group_id = self.consumer_group_id)
        if self.old_consumers != consumers:

            all_process=0
            jobs = []
            if self.p:
                for self.p in jobs: 
                    self.p.terminate()

            for consumer in consumers:
                kafka_worker = []
                for i in range(int(consumer[6])):
                    all_process+=1
                    kafka_worker.append(Kafka(
                                    topic_name = consumer[1],
                                    group_id = consumer[0],
                                    auto_offset_reset = consumer[5]
                                    ))
                    
                for kafka_object in kafka_worker:
                    self.p = multiprocessing.Process(target=self.elasticsearch_instance.post(
                                                                                            kafka_object.consume(),
                                                                                            consumer[2]) 
                                                                                            )  # consumer[2] : consumer['elastic_index']
                    jobs.append(self.p)
                    self.p.start()
                
                print(all_process)
                self.old_consumers = consumer 

        else:
            time.sleep(60)
         

        return jobs