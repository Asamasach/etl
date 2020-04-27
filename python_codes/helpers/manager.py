from helpers.mysqlHelper import Mysql
from logHelper import Log
from helpers.consumerHelper import Consumer
import time

old_groups = []
old_consumers = []
consumers_group = []
mysql_instance = Mysql()
manager_log = Log('notification')

if __name__ == "__main__":

    groups = mysql_instance.get_groups()

    if old_groups != groups:
        for i in groups:
            group_id = groups[i][0]
            consumers_group[i] = Consumer(group_id)
            manager_log.write("{} group_id has been detected!".format(group_id),"Manager")
        old_consumers = consumers_group
    
    for consumer in consumers_group:
        consumer.consume()
        time.sleep(1)