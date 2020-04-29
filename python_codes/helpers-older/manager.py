from mysqlHelper import Mysql
from logHelper import Log
from consumerHelper import Consumer
import time

old_groups = []
old_consumers = []
consumers_group = []
mysql_instance = Mysql()
manager_log = Log('notification')

if __name__ == "__main__":

    groups = mysql_instance.get_groups()

    if old_groups != groups:
        for i in range(len(groups)):
            group_id = groups[i][0]
            consumers_group.append(Consumer(group_id))
            message = "new group_id has been detected!" #  .format(group_id)
            print(message)
            #manager_log.write(message,"Manager")
        old_groups = groups

    while old_groups == groups: 
        for consumer in consumers_group:
            consumer.consume()
           # time.sleep(1)
