from pyarrow.parquet import ParquetFile
import pyarrow as pa
import pandas as pd
import os
import sys
import re
from connect import connect
from send_email import Mail
import logging

def source_data():

        conn_temp = connect()
        cursor = conn_temp[0].cursor()
        cursor.execute('SELECT year, month from sourcedata')
        source_config_data = cursor.fetchone()
        return source_config_data

a = source_data()
#path="/home/anuradha.mishra/Untitled Folder/epl-pipeline_project/etl-pipeline/etl_pipeline/data/year"
class CollectingData:
    def access_file_collecting(self):

        try: 
            path="../data/year"
            a=source_data()
            # year,month=input("Enter year month separated with ,:").split(",")
            relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])
            relative_parquet_path=relative_new_path+"/"+"parquet"
            self.parquet_toCsv_with_partitioned_data(path1=relative_new_path,path2=relative_parquet_path)
        except Exception as e:
            mail = Mail()
            mail.initiate_email('Parquet_toCsv_with_partitioned_data!',e,0)
            return None
            


    def parquet_toCsv_with_partitioned_data(self,path1="",path2=""):
        try: 
            a=source_data()
            root_path=path1
            parquet_path=path2
            if (os.path.exists(root_path)) and (os.path.exists(parquet_path)):
                for file in os.listdir(parquet_path):
                    pf=ParquetFile(parquet_path+"/"+str(file))
                    first_hun_rows=next(pf.iter_batches(batch_size=200))
                    df=pa.Table.from_batches([first_hun_rows]).to_pandas()
                    csv_dump_path=root_path+"/"+"csv"+"/"
                    df.to_csv(csv_dump_path+file.replace('.parquet','.csv'))
                # print("Conversion Completed") 
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("Parquet is converted to CSV with 200 partitions!") 
            else:
                logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
                logger=logging.getLogger(__name__)
                logger.info("No parquet file is found for conversion!")    
        except Exception as e:
            #print(e)
            #print(sys.exc_type)
            mail = Mail()
            mail.initiate_email('Parquet_toCsv_with_partition_data',e,0)
            return 
        return
