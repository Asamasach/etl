import unittest
import logging

class Log():    

    def __init__(self,log_type):
        self.log_type = log_type

    def write(self,message,service_name):
        logging.basicConfig(filename='../logs/'+str(service_name)+'.log',
                            filemode='w',
                            format='%(asctime)s - %(log_type)s - %(service_name)s - %(message)s',
                            level=logging.INFO
                            )
        logging.info(message)
        
    # def kafka(self,message):
    #     logging.basicConfig(filename='../logs/kafka.log',
    #                         filemode='w',
    #                         format='%(asctime)s - %(log_type)s - %(message)s',
    #                         level=logging.INFO
    #                         )
    #     logging.info(message)
