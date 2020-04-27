
# from helpers.kafkaHelper import Kafka
from helpers.mysqlHelper import Mysql
# from logHelper import Log
from helpers.consumerHelper import Consumer
import time

old_groups = []
old_consumers = []
consumers_group = []
mysql_instance = Mysql()

if __name__ == "__main__":

    groups = mysql_instance.get_groups()

    if old_groups != groups:
        for group_id in groups:

            # consumers[group_id] = mysql.get_list(group_id = group_id)
            consumers_group[group_id] = Consumer(group_id)

        old_consumers = consumers_group
    
    for consumer in consumers_group:
        consumer.consume()
        time.sleep(1)