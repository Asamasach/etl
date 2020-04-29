from kafkaHelper import Kafka
from mysqlHelper import Mysql
from logHelper import Log
from multiprocessing import Process
import time

mysql_instance = Mysql()
process_id = 0

old_groups = []
old_consumers = []
consumers_group = []
jobs = []


while True:

        consumers = mysql_instance.get_list()
        if old_consumers != consumers:
            groups = mysql_instance.get_groups()
            for i in range(len(groups)):
                group_id = groups[i][0]
                print("(group_id: {} ) has been detected!".format(group_id))
            process_id=0
            if jobs:
                print("we have {} processes which are going to be terminated".format(jobs))
                for p in jobs: 
                    p.terminate()
                jobs = []


            for consumer in consumers:
                if str(consumer[7]) == '1':

                    kafka_workers = []
                    print(consumer)
                    for _ in range(int(consumer[6])):
                        kafka_workers.append(Kafka(
                                        topic_name = consumer[1],
                                        group_id = consumer[0],
                                        auto_offset_reset = consumer[5],
                                        kafka_id = process_id
                                        ))
                        print("{}th process of consumer_id {} added to jobs".format(process_id, consumer[3])) 
                        process_id+=1
                
                    for kafka_worker in kafka_workers:
                        p = Process(target=kafka_worker.consume(consumer[2], consumer[3]))   # consumer[2] : consumer['elastic_index'] # id
                        jobs.append(p)
                    print("number of running process are : {}".format(process_id))
                    for p in jobs:
                        p.start()
                        p.join()

                else:
                    print("consumer_id {} is disabled".format(consumer[3])) #,"consumer")

            old_consumers = consumers
        else:
            time.sleep(1)
