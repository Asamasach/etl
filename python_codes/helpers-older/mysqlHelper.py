# add password in config file
# add privileges of current user and ip to mysql
# create user : CREATE USER 'user'@'host' IDENTIFIED BY 'user_password'; (better off find a way to implement in init_db())
# grant all privileges on *.* to 'user'@'host' with grant option;


import mysql.connector
from logHelper import Log
import yaml
import os

fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/mysql.yml")

class Mysql:


    def __init__(self):
        
        with open(config_file_path) as mysql_conf:
            self.conf = yaml.load(mysql_conf, Loader=yaml.FullLoader)
        # self.row_count = 0

    def connect_mysql(self):
        
        self.connection = mysql.connector.connect(
                                                    host = self.conf['host'],
                                                    database = self.conf['database'],
                                                    user = self.conf['user'],
                                                    password = self.conf['password']
                                                    )
        
        if self.connection.is_connected():
            connected = True
        else:
            connected = False
        
        return connected
    
    def disconnect_mysql(self):
        
        self.connection.disconnect()
        
        return None

    def insert_record(self,
                group_id = 'mysql_helper_group_id',
                topic_name = '',
                elastic_index = 'mysql_helper',
                consumer_name = '',
                auto_offset_reset = 'earliest',
                process_number = '1',
                consumer_en = '1'
                ):

        connected = self.connect_mysql() 

        if connected:
            cursor = self.connection.cursor()
            my_sql_insert_record = """ INSERT INTO {} (
                                        group_id,
                                        topic_name,
                                        elastic_index,
                                        consumer_name,
                                        auto_offset_reset,
                                        process_number,
                                        consumer_en) VALUES (%s, %s, %s, %s, %s, %s, %s) ;"""
            
            my_sql_insert_record_query = my_sql_insert_record.format(self.conf['table']) 

            cursor.execute(my_sql_insert_record_query, (group_id, topic_name, elastic_index, consumer_name, auto_offset_reset, process_number, consumer_en))   
            self.connection.commit()    

            row_count = cursor.rowcount
            
        cursor.close()
        
        return row_count


    def get_list(self,
            group_id = '%',
            topic_name = '%',
            elastic_index = '%',
            consumer_id = '%',
            consumer_name = '%',
            auto_offset_reset = '%',
            process_number = '%',
            consumer_en = '1'
            ):

        connected = self.connect_mysql() 

        if connected:
            cursor = self.connection.cursor()
            my_sql_select_total = """ SELECT * FROM {} WHERE
                                        group_id LIKE %s AND
                                        topic_name LIKE %s AND
                                        elastic_index LIKE %s AND
                                        consumer_id LIKE %s AND
                                        consumer_name LIKE %s AND
                                        auto_offset_reset LIKE %s AND
                                        process_number LIKE %s AND
                                        consumer_en LIKE %s ;"""
            
            my_sql_select_total_query = my_sql_select_total.format(self.conf['table']) 

            cursor.execute(my_sql_select_total_query, (group_id, topic_name, elastic_index, consumer_id, consumer_name, auto_offset_reset, process_number, consumer_en))   
            result=cursor.fetchall()

            # self.row_count = cursor.rowcount
            
        cursor.close()
            
        return result

    def get_groups(self):

        connected = self.connect_mysql() 
            
        if connected:
            cursor = self.connection.cursor()
            my_sql_select_total = """ SELECT DISTINCT group_id FROM {} ;"""            
            cursor.execute(my_sql_select_total.format(self.conf['table']))  
            result=cursor.fetchall()

        cursor.close()
            
        return result

    def init_db(self):

        init_connection = mysql.connector.connect(
                                            host = self.conf['host'],
                                            user = self.conf['user'],
                                            password = self.conf['password']
                                            )
        if init_connection.is_connected():
            cursor = init_connection.cursor()
            my_sql_create_database = """ CREATE DATABASE {} ;"""
            database_create_query = my_sql_create_database.format(self.conf['database'])
            cursor.execute(database_create_query)
            
        cursor.close()
        init_connection.close()
        return None

    def init_table(self):
        connected = self.connect_mysql()
        if connected:
            cursor = self.connection.cursor()
            my_sql_create_table = """ CREATE TABLE {} (
                                        group_id VARCHAR(255) ,
                                        topic_name VARCHAR(255) ,
                                        elastic_index VARCHAR(255) ,
                                        consumer_id INT AUTO_INCREMENT PRIMARY KEY ,
                                        consumer_name VARCHAR(255) ,
                                        auto_offset_reset VARCHAR(255) ,
                                        process_number INT,
                                        consumer_en BOOLEAN 
                                        ) ;"""

            table_create_query = my_sql_create_table.format(self.conf['table'])
            cursor.execute(table_create_query)
            
        self.disconnect_mysql()
