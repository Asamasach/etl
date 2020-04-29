import sys
from kafkaHelper import Kafka
from mysqlHelper import Mysql
import os

def main():
    mysql_instance = Mysql()

    consumer_id = sys.argv[1]
    print(consumer_id)
    process_number = sys.argv[2]
    print(process_number)
    consumer = mysql_instance.get_list( consumer_id= consumer_id)[0]
#    print(consumer[0])
    group_id = consumer[0]
    topic_name = consumer[1]
    index = consumer[2]
    auto_offset_reset = consumer[5]

    kafka = Kafka(
                    topic_name = topic_name,
                    group_id = group_id,
                    auto_offset_reset = auto_offset_reset,
                    kafka_id = process_number
                )
    f = open("/tmp/{}-pid.txt".format(process_number), "w")
    f.write(str(os.getpid()))
    print(os.getpid())
    f.close()

    kafka.consume(
                  index = index,
                  consumer_id = consumer_id
                  )


main()
