# a calss for elasticsearch
from elasticsearch import Elasticsearch    
import yaml
from logHelper import Log
import unittest
import os
from datetime import datetime


fileDir = os.path.dirname(os.path.realpath('__file__'))
config_file_path = os.path.join(fileDir,  "config/elastic.yml")

class Elastic():

    def __init__(self):

        with open(config_file_path) as elastic_conf:
            conf = yaml.load(elastic_conf, Loader=yaml.FullLoader)
        
        self.es =  Elasticsearch(
                            [str(conf['host'])],
                            http_auth = str(conf['user'])+":"+str(conf['pass']),
                            timeout = conf['connection_timeout'],
                            max_retries = conf['max_retries'],
                            retry_on_timeout = conf['retry_on_timeout']
                            )

    def post(self, data, index):
        self.data = data
        self.index = index
        data_message_count = 0 
        successful_indexed_count = 0 
        for doc in data:
#            doc.stamp_time()
            stamped_data = {
                                'data': doc ,
                                'timestamp' : datetime.now()
                                }

            data_message_count += 1  #calculate messages in data 
            self.post_result = self.es.index(index= self.index, body=stamped_data)  #add documnets in elasticsearch, in index related
            if (str(self.post_result['result']) == "created"): 
                successful_indexed_count += 1  #count number of successful objects sent to elasticsearch
            else:            
                err = Log("Error")
                err.write(self.post_result,"elastic") 
                print(str(self.post_result['result']))
        #    self.es.indices.refresh(index= self.index) # refresh index
        #try:
        #    test_c=unittest.TestCase()
        #    test_c.assertEqual(data_message_count, successful_indexed_count)
        #except AssertionError:
        #    err = Log('Error') 
        #    err.write('%(data_message_count)s are counted, but %(data_message_count)s are sent to elastic','elastic')
        #    pass
        return data_message_count, successful_indexed_count
    
    def get(self, index):
        get_result = self.es.get(index=index, id=1)
        return(get_result['_source'])

#    def stamp_time(self):
#        self.stamped_data = self.data
#        from datetime import datetime
#        for doc in self.data:
#            self.stamped_data = {
#                                'data': self.data,
#                                'timestamp' : datetime.now()
#                                }
#            print(doc)
#        return self.stamped_data
