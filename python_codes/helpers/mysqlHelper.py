import mysql.connector
from helpers.logHelper import Log
import yaml

class Mysql:


    def __init__(self):
        
        with open(r'../config/mysql.yml') as mysql_conf:
            self.conf = yaml.load(mysql_conf)
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

    def get_list(self,
                group_id = '%',
                topic_name = '%',
                elastic_index = '%',
                consumer_id ='%',
                consumer_name = '%',
                auto_offset_reset = '%',
                process_number = '%',
                consumer_en = '1'
                ):

        connected = self.connect_mysql() 

        if connected:
            cursor = self.connection.cursor()
            my_sql_select_total = """ SELECT * FROM %s WHERE
                                        group_id = %s AND
                                        topic_name = %s AND
                                        elastic_index = %s AND
                                        consumer_id = %s AND
                                        consumer_name = %s AND
                                        auto_offset_reset = %s AND
                                        process_number = %s AND
                                        consumer_en = %s """
            
            table_name = self.conf['table']
            
            cursor.execute(my_sql_select_total,
                                (
                                table_name,
                                group_id,
                                topic_name,
                                elastic_index,
                                consumer_id,
                                consumer_name,
                                auto_offset_reset,
                                process_number,
                                consumer_en
                                )
                            )   
            result=cursor.fetchall()

            # self.row_count = cursor.rowcount
        
        cursor.close()
        
        return result

    def get_groups(self):

        connected = self.connect_mysql() 
            
        if connected:
            cursor = self.connection.cursor()
            my_sql_select_total = """ SELECT DISTINCT group_id FROM %s """            
            cursor.execute(my_sql_select_total, self.conf['table'])   
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
            my_sql_create_database = """ CREATE DATABASE %s """
            cursor.execute(my_sql_create_database, self.conf['database'])
        
        cursor.close()
        init_connection.disconnect()
        return None

    def init_table(self):
        connected = self.connect_mysql()
        if connected:
            cursor = self.connection.cursor()
            my_sql_create_table = """ CREATE TABLE %s (
                                        group_id VARCHAR(255) ,
                                        topic_name VARCHAR(255) ,
                                        elastic_index VARCHAR(255) ,
                                        consumer_id INT AUTO_INCREMENT PRIMARY KEY ,
                                        consumer_name VARCHAR(255) ,
                                        auto_offset_reset BOOLEAN ,
                                        process_number INT,
                                        consumer_en BOOLEAN 
                                        ) """

            cursor.execute(my_sql_create_table, self.conf['table'])
        
        self.disconnect_mysql()