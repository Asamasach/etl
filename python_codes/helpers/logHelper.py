import logging

class Log():    

    def __init__(self,log_type_):
        self.log_type_ = log_type_

    def write(self, message, service_name):
        filename = 'logs/'+str(service_name)+'.log'
        f = open(filename, "w")
        f.close()
        logging.basicConfig(filename= filename,
                            filemode='a',
                            format='%(asctime)s - %(self.log_type_)s - %(service_name)s - %(message)s',
                            level=logging.INFO
                            )
        logging.info(message)
