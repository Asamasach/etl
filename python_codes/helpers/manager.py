from kafkaHelper import Kafka
from mysqlHelper import Mysql
from logHelper import Log
from multiprocessing import Process
import time
import os

mysql_instance = Mysql()
process_id = 0
consumers = []
old_groups = []
old_consumers = []
consumers_group = []
jobs = []
old_jobs = []

while True:

        consumers = mysql_instance.get_list()
        if old_consumers != consumers:
            groups = mysql_instance.get_groups()
            for i in range(len(groups)):
                group_id = groups[i][0]
                print("(group_id: {} ) has been detected!".format(group_id))
            if jobs:
#                print("we have {} processes which are going to be terminated".format(jobs))
                remove_jobs = []
                for consumer in consumers:
                    if consumer != old_consumers[consumers.index(consumer)]:
#                    if str(consumer[7]) == '0':
                        for process_id_no in jobs: 
                            if process_id_no[0] == int(consumer[3]):
                                remove_jobs.append([process_id_no[0],process_id_no[1]])
                                print("job {} has removed!".format(process_id_no))
                for stop_process in remove_jobs:
    
                    os.system('kill $(cat /tmp/{}-{}-pid.txt)'.format(stop_process[0] ,stop_process[1]))
                    jobs.remove(stop_process)
                               
            print("jobs after termination are: {}".format(jobs))


            for consumer in consumers:
               # if consumer != old_consumers[consumers.index(consumer)]:
                 
                if str(consumer[7]) == '1':

                    #kafka_workers = []
                    print(consumer)
                    process_number = 0
                    process_id = 0
                    exist_flag = 0
                    if jobs:
                        for process_id_no in jobs:

                            if process_id_no[0] == consumer[3]:
                                exist_flag = 1
                                break
                    if exist_flag == 0:
                        for _ in range(int(consumer[6])):
                            print("{}th process of consumer_id {} added to jobs".format(process_number, consumer[3])) 
                            process_id+=1
                            process_number+=1
                            os.system('nohup /usr/bin/python3 ./kafka-consumer.py {} {} > /dev/null 2> /dev/null &'.format(consumer[3] , process_number))
                            jobs.append([consumer[3], process_number])
                        print(jobs)
                        print("number of running process are : {}".format(process_number))
                        exist_flag = 1
                    # for p in jobs:
                    #     p.start()
                    #     p.join()
                        print("current jobs are: {}".format(jobs))
                else:
                    print("consumer_id {} is disabled".format(consumer[3])) #,"consumer")

            old_consumers = consumers
        else:
            time.sleep(1)
