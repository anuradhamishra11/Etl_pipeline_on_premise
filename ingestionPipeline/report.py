import pandas as pd
import os
from connect import connect
from collecting_data import source_data
from send_email import Mail
import logging


class dummy_report():
    """This class create dummy_report by reading transform csv files"""

    def maxloc10(self,dataset):
        """This function return maximum duration """
        a = source_data()
        path="../data/year"
        relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/updated_data/duration/"
        df = pd.read_csv(relative_new_path + dataset)
        max_duration = df.sort_values(by="duration",ascending=False).head(10)
        top_10_Maxduration = max_duration['duration'].tolist()
        return top_10_Maxduration

    def minloc10(self,dataset):
        """This function return minimum duration """
        a = source_data()
        path="../data/year"
        relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/updated_data/duration/"
        df = pd.read_csv(relative_new_path + dataset)
        min_duration = df.sort_values(by="duration").head(10)
        top_10_Minduration = min_duration['duration'].tolist()
        return top_10_Minduration

    def mostused_location(self,dataset):
        """This function return most uses location """
        a = source_data()
        path="../data/year"
        relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/updated_data/location/"
        df = pd.read_csv(relative_new_path + dataset)

        ten_used_location = pd.DataFrame(df.DOlocation.value_counts())
        ten_used_location = ten_used_location.drop('Not Present')
        top_used_loc = ten_used_location.head(10)
        top_used_loc = list(top_used_loc.index)
        return top_used_loc

    def durationcsvfiles(self):
        """This function read all duration csv files and perform some operations for creating reports """
        a = source_data()
        relative_new_path="../data/year"+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/updated_data/duration/"
        for file in os.listdir(relative_new_path):
            maxloc = self.maxloc10(file)
            minloc = self.minloc10(file)
            self.storingdbreportd(file,maxloc,minloc)

    def locationcsvfiles(self):
        """This function read all location csv files and perform some operations for creating reports """
        a = source_data()
        relative_new_path="../data/year"+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/updated_data/location/"
        for file in os.listdir(relative_new_path):
            loc = self.mostused_location(file)
            self.storingdbreportl(file, loc)
        logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logger=logging.getLogger(__name__)
        logger.info("Reports_are_created!")
        e = 'data pipeline working'
        mail = Mail()
        mail.initiate_email('Success',e,1)

    def storingdbreportd(self,file_name,maxduration,minduration):
        """This function store all duration reports inside database """
        conn_temp = connect()
        if conn_temp[1]=="postgresql":
            cursor = conn_temp[0].cursor()
            cursor.execute("INSERT INTO durationreports (file_name,max_duration,min_duration) VALUES(%s, %s, %s)", (file_name, maxduration, minduration))
        elif conn_temp[1]=="mssql":
            cursor = conn_temp[0].cursor()
            cursor.execute("INSERT INTO durationreports (file_name,max_duration,min_duration) VALUES(?, ?, ?)", (str(file_name), str(maxduration), str(minduration)))
        conn_temp[0].commit()

    def storingdbreportl(self,file_name,usesloc):
        """This function store all location reports inside database """
        conn_temp = connect()
        if conn_temp[1]=="postgresql":
            cursor = conn_temp[0].cursor()
            cursor.execute("INSERT INTO locationreports (file_name,Most_Uses_location) VALUES(%s, %s)", (file_name, usesloc))
        elif conn_temp[1]=="mssql":
            cursor = conn_temp[0].cursor()
            cursor.execute("INSERT INTO locationreports (file_name,Most_Uses_location) VALUES(?,?)", (str(file_name), str(usesloc)))
        conn_temp[0].commit()

# obj = dummy_report()
# obj.durationcsvfiles()
# obj.locationcsvfiles()

