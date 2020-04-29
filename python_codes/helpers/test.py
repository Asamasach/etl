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

main()
