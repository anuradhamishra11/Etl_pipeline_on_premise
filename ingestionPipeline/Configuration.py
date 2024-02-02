import random
from subprocess import call
from connect import connect
from Collect_source_data import *
from collecting_data import *
from pandas_transform import *
from report import *
from Data_cleaning import *
from sftp import *


class Call(object):

    @staticmethod
    def source_data_new():
        conn_temp = connect()
        cursor = conn_temp[0].cursor()
        cursor.execute("SELECT keys,status FROM configuration where status=1")
        records=cursor.fetchall()
        return records


obj=Call()
data=obj.source_data_new()

for i in data:
    if i[0]=='delete_csv' and i[1]==1:
        obj=Datacleaning()
        obj.delete_csv()
    elif i[0]=='delete_parquet' and i[1]==1:   
        obj=Datacleaning()
        obj.delete_parquet()
    elif i[0]=='delete_updated_data' and i[1]==1:
        obj=Datacleaning()
        obj.delete_Updated_data() 
    elif i[0]=='delete_logs' and i[1]==1:
        obj=Datacleaning()    
        obj.delete_logs()  
    elif i[0]=='delete_duration_data' and i[1]==1:
        obj=Datacleaning()
        obj.delete_duration_data()
    elif i[0]=='delete_location_data' and i[1]==1:
        obj=Datacleaning()
        obj.delete_location_data()
    elif i[0]=='collect_data_from_source' and i[1]==1:
        obj=Collectsourcedata()
        obj.access_file()       
    elif i[0]=='converting_parquet_to_csv' and i[1]==1:
        obj=CollectingData()
        obj.access_file_collecting()  
    elif i[0]=='transform_data' and i[1]==1:
        obj=AddingColumn()
        obj.csvfiles()   
    elif i[0]=='report_generate' and i[1]:
        obj=dummy_report()
        obj.durationcsvfiles()
        obj.locationcsvfiles()
    elif i[0]=='data_fetch' and i[1]==1:  
        obj=sftp_connection()
        obj.data_from_sftp()  


    
    








