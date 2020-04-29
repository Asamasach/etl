from kafkaHelper import Kafka
from mysqlHelper import Mysql
from logHelper import Log
from multiprocessing import Process
import time



#         self.old_consumers = []
#         self.p = []
        
# #        self.log = Log("consumerHelper")
#         self.jobs = []


mysql_instance = Mysql()
all_process = 0

old_groups = []
old_consumers = []
consumers_group = []
jobs = []


if __name__ == "__main__":

    groups = mysql_instance.get_groups()
    if old_groups != groups:
        for i in range(len(groups)):
            group_id = groups[i][0]
            print("(group_id: {} ) has been detected!".format(group_id)) #  .format(group_id)
            consumers = mysql_instance.get_list(group_id = group_id)
            if old_consumers != consumers:
                all_process=0
                if jobs:
                    print("we have {} processes which are going to be terminated".format(self.jobs))
                    for p in jobs: 
                        p.terminate()
                    jobs = []


                for consumer in consumers:
                    if str(consumer[7]) == '1':

                        kafka_worker = []
                        print(consumer)
                        for _ in range(int(consumer[6])):
                            all_process+=1
                            kafka_worker.append(Kafka(
                                            topic_name = consumer[1],
                                            group_id = consumer[0],
                                            auto_offset_reset = consumer[5]
                                            ))
                        print("consumer_id {} is running now".format(consumer[3])) 
                    
                        for kafka_object in kafka_worker:
                            p = Process(target=kafka_object.consume(consumer[2], consumer[3]))   # consumer[2] : consumer['elastic_index'] # id
                            jobs.append(p)
                        print("number of running process are : {}".format(all_process))
                    else:
                        print("consumer_id {} is disabled".format(consumer[3])) #,"consumer")
                for p in jobs:
                    p.start()
                    p.join()

                old_consumers = consumers
            else:
                time.sleep(1)
                        
        old_groups = groups

    else:
        time.sleep(1)