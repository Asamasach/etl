from kafkaHelper import Kafka
from mysqlHelper import Mysql
from elasticHelper import Elastic
from logHelper import Log
from multiprocessing import Process
import time

class Consumer():

    def __init__(self, consumer_group_id):
        self.consumer_group_id = consumer_group_id

        self.old_consumers = []
        self.p = []
        self.elasticsearch_instance = Elastic()
#        self.log = Log("consumerHelper")
        self.jobs = []
    def consume(self):

        consumers = []
        mysql = Mysql()
        consumers = mysql.get_list(group_id = self.consumer_group_id)
        all_process = 0
        if self.old_consumers != consumers:
        

            all_process=0
#            jobs = []
            if self.jobs:
                print("we have processes which going to terminate")
                for self.p in self.jobs: 
                    self.p.terminate()
            
                self.jobs = []
            for consumer in consumers:
                if str(consumer[7]) == '1':

                    kafka_worker = []
                    print(consumer)
                    for i in range(int(consumer[6])):
                        all_process+=1
                        kafka_worker.append(Kafka(
                                        topic_name = consumer[1],
                                        group_id = consumer[0],
                                        auto_offset_reset = consumer[5]
                                        ))
                    print("consumer_id {} is running now".format(consumer[3])) 
                       
                    for kafka_object in kafka_worker:
                        self.p = Process(target=self.elk_consume(
                                                                                                kafka_object.consume(),
                                                                                                consumer[2]) 
                                                                                                )  # consumer[2] : consumer['elastic_index']
                        self.jobs.append(self.p)
                    print("number of running process are : {}".format(all_process))
                else:
                    self.log.write("consumer_id {} is disabled".format(consumer[3]),"consumer")
            for self.p in self.jobs:
                self.p.start()
                self.p.join()

            self.old_consumers = consumers
        else:
            time.sleep(1)

        return all_process
    def elk_consume(self, data, index):
        self.elasticsearch_instance.post(data= data, index= index)
        print("elk_consume for index : {} and data: {}".format(index, data))
        return None
