from confluent_kafka.admin import AdminClient, NewTopic
from mysqlHelper import Mysql
import os
import yaml

mysql = Mysql()

fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/kafka.yml")

with open(config_file_path) as kafka_conf:
    conf = yaml.load(kafka_conf, Loader=yaml.FullLoader)

admin_client = AdminClient({ 'bootstrap.servers' : conf['bootstrap_servers']})

topic_list = []
consumers = mysql.get_list()
for consumer in consumers:
    topic_list.append(NewTopic(str(consumer[1]), consumer[6], conf['replicas']))

admin_client.create_topics(topic_list)
