from collecting_data import source_data
from pyarrow.parquet import ParquetFile
import pyarrow as pa
import pandas as pd
import os
import sys
import re
from links import tlc_data
import requests
import logging
from send_email import Mail
from connect import connect


class Collectsourcedata:

    # path="/home/brijender.kushwaha/Data_Science/Etl_pipeline/data/year"

    def access_file(self):
        try: 
            a = source_data()
            path="../data/year"
            # year,month=input("Enter year month separated with ,:").split(",")
            relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])
            relative_parquet_path=relative_new_path+"/"+"parquet"
            self.collecting_parquet_data_tlc(tlc_data,path1=relative_parquet_path)
        except Exception as e:
            # print(e)
            # print(sys.exc_type)
            mail = Mail()
            mail.initiate_email('No paths found',e,0)



    def collecting_parquet_data_tlc(self,data,path1=''):
        root_path=path1
        try:
            a = source_data()
            tlc_datas=data[str(a[0])][a[1]]
            logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
            logger=logging.getLogger(__name__)
            logger.info("Parquet file found in source!")
        except FileNotFoundError:
            mail = Mail()
            mail.initiate_email('Collecting_parquet_data',e,0)
            return    
        for key,value in tlc_datas.items():
            r=requests.get(value)
            file_name=value.split("/")[-1]
            try:
                new_path=""
                new_path=root_path+"/"
                with open(os.path.join(new_path,file_name),'wb') as f:
                    f.write(r.content)
                    #print("{} file is downloaded".format(file_name))
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("Source Data Downloaded")    
            except Exception as e:
                #  logging.basicConf(filename="/Logs/collecting_parquet_data/collecting_parquet_data.log",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name(s %(message)s")
                #  logger=logging.getLogger(__name__)
                #  logger.error(e)
                #  print(e)
                #  print(sys.exc_type)
                mail = Mail()
                mail.initiate_email('Collecting_source_data',e,0)
                return None
               
        return

