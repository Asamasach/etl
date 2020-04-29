from kafkaHelper import Kafka
from mysqlHelper import Mysql
from logHelper import Log
from multiprocessing import Process
import time
import os

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
                for process_id_no in jobs: 
                    os.system('kill $(cat /tmp/{}-pid.txt)'.format(process_id_no))
                jobs = []


            for consumer in consumers:
                if str(consumer[7]) == '1':

                    kafka_workers = []
                    print(consumer)
                    process_number = 0
                    for _ in range(int(consumer[6])):

                        print("{}th process of consumer_id {} added to jobs".format(process_number, consumer[3])) 
                        process_id+=1

                        # p = Process(target=kafka_worker.consume(consumer[2], consumer[3]))   # consumer[2] : consumer['elastic_index'] # id
                        os.system('nohup /usr/bin/python3 ./kafka-consumer.py {} {} > /dev/null 2> /dev/null &'.format(consumer[3] , process_id))
                        jobs.append(process_id)
                    print("number of running process are : {}".format(process_id))
                    # for p in jobs:
                    #     p.start()
                    #     p.join()

                else:
                    print("consumer_id {} is disabled".format(consumer[3])) #,"consumer")

            old_consumers = consumers
        else:
            time.sleep(1)
