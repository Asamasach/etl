import yaml
import os

fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/mysql.yml")
with open(config_file_path) as mysql_conf:
    conf = yaml.load(mysql_conf, Loader=yaml.FullLoader)

print(conf)
print("----")
print(conf['host'])
