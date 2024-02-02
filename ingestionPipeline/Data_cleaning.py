from collecting_data import *
import os
import glob
from datetime import datetime
from random import randint
import smtplib, ssl
from connect import connect

class Datacleaning(object):
    
    a=source_data()
    path="../data/year"
    relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])

    def delete_parquet(self):
        relative_parquet_path=self.relative_new_path+"/"+"parquet"+"/*.parquet"
        files=glob.glob(relative_parquet_path)
        try:
            for file in files:
                os.remove(file)
            logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
            logger=logging.getLogger(__name__)
            logger.info("Parquet data are deleted from database!")
        except Exception as e:
            mail = Mail()
            mail.initiate_email('No parquet file found',e,0)
                 
    
    def delete_csv(self):
        relative_csv_path=self.relative_new_path+"/"+"csv"+"/*.csv"
        files=glob.glob(relative_csv_path)
        try:
            for file in files:
                os.remove(file)
            logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
            logger=logging.getLogger(__name__)
            logger.info("Logs data are deleted from database!")    
        except Exception as e:
            mail = Mail()
            mail.initiate_email('No csv file found',e,0)    

    def delete_Updated_data(self):
        relative_csv_path_updated=self.relative_new_path+"/"+"updated_data"+"/"
        duration_path=relative_csv_path_updated+"duration"+"/*.csv"
        location_path=relative_csv_path_updated+"location"+"/*.csv"
        file1=glob.glob(duration_path)
        file2=glob.glob(location_path)
        files_paths=[file1,file2]
        try:
            for paths in files_paths:
                for file in paths:
                    os.remove(file)
            logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
            logger=logging.getLogger(__name__)
            logger.info("tranformed data are deleted from database!")        
        except Exception as e:
            mail = Mail()
            mail.initiate_email('No csv file found',e,0)   

    def delete_logs(self):
        try:
            conn_temp = connect()
            cursor = conn_temp.cursor()
            cursor.execute('Delete from runlog')
            conn_temp.commit()
            cursor.execute('select * from runlog')
            records=cursor.fetchall()
            if len(records)==0:
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("Logs data are deleted from database!")
        except Exception as e:
            #print(e)
            #print(sys.exc_type)
            mail = Mail()
            mail.initiate_email('failure in log deletion',e,0)
            return         

    def delete_location_data(self):
        try:
            conn_temp = connect()
            cursor = conn_temp.cursor()
            cursor.execute('Delete from locationreports')
            conn_temp.commit()
            cursor.execute('select * from locationreports')
            records=cursor.fetchall()
            if len(records)==0:
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("Location data are deleted from database!")
        except Exception as e:
            #print(e)
            #print(sys.exc_type)
            mail = Mail()
            mail.initiate_email('Failure in deleting location data',e,0)
            return                       

    def delete_duration_data(self):
        try:
            conn_temp = connect()
            cursor = conn_temp.cursor()
            cursor.execute('Delete from durationreports')
            conn_temp.commit()
            cursor.execute('select * from durationreports')
            records=cursor.fetchall()
            if len(records)==0:
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("duration data are deleted from database!")
        except Exception as e:
            #print(e)
            #print(sys.exc_type)
            mail = Mail()
            mail.initiate_email('Failure in deleting duration data',e,0)
            return                       
      


