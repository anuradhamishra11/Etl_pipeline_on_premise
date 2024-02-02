from lib2to3.pgen2.token import EQUAL
import pandas as pd
import numpy as np
from collecting_data import source_data
import os
from send_email import Mail
import logging

class AddingColumn:
    """ This class adding two location and duration column in dataframes and store them in a folder """

    def addDurationCol(self,dataset):
        """ This function add duration column inside dataset"""
        try:
            a = source_data()
            path="../data/year"
            relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/csv/"
            df = pd.read_csv(relative_new_path + dataset)
            self.dataset_Column_Changes(df)

            df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
            df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
            df['duration'] = df['dropoff_datetime'] - df['pickup_datetime']
            df['duration'] = df['duration'].apply(lambda x: str(x)[6:])

            csv_dump_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/"+"updated_data/duration"+"/"
            df.to_csv(csv_dump_path+"updated_"+dataset)
            
            return df
        except Exception as e:
            mail = Mail()
            mail.initiate_email('AddLocationCol',e,0)
            return None

    def addLocationCol(self,dataset):
        """ This function add Location column inside dataset """
        try:
            def locationName(col):
                """This function return list, inside list it's store location name by location id """
                location = []
                for i in col:
                    # if i==0.0:
                    #     location.append('Not Present')
                    if i==85.0:
                        location.append('Depot Greater Noida')
                    elif i==188.0:
                        location.append('GNIDA office')
                    elif i== 61.0:
                        location.append('Delta 1 Greater Noida')
                    elif i==76.0:
                        location.append('Alpha 1 Greater Noida')
                    elif i==67.0:
                        location.append('Pari Chowk')
                    elif i== 155.0:
                        location.append('Knowledge Park II')
                    elif i==14.0:
                        location.append('Noida Sector 148')
                    elif i==65.0:
                        location.append('Noida Sector 147')
                    elif i== 198.0:
                        location.append('Noida Sector 146')
                    elif i==232.0:
                        location.append('Noida Sector 145')
                    elif i==195.0:
                        location.append('Noida Sector 144')
                    elif i== 17.0:
                        location.append('Noida Sector 143')
                    elif i==234.0:
                        location.append('Noida Sector 142')
                    elif i==45.0:
                        location.append('Noida Sector 137')
                    elif i== 42.0:
                        location.append('Noida Sector 183')
                    elif i==256.0:
                        location.append('NSEZ Noida')
                    elif i==168.0:
                        location.append('Noida Sector 81')
                    elif i== 16.0:
                        location.append('Noida Sector 101')
                    elif i==262.0:
                        location.append('Noida Sector 76')
                    elif i==113.0:
                        location.append('Noida Sector 50')
                    elif i== 229.0:
                        location.append('Noida Sector 51')
                    elif i==265.0:
                        location.append('Noida Sector 52')
                    elif i==213.0:
                        location.append('Noida Sector 34')
                    elif i== 212.0:
                        location.append('Okhla Bird Sanctuary')
                    elif i==167.0:
                        location.append('Noida City Center')
                    elif i==78.0:
                        location.append('Golf Course')
                    elif i== 94.0:
                        location.append('Botanical Garden')
                    elif i==69.0:
                        location.append('Noida Sector 18')
                    elif i==244.0:
                        location.append('Noida Sector 16')
                    elif i== 127.0:
                        location.append('Noida Sector 15')
                    elif i==169.0:
                        location.append('Kalindi Kunj')
                    elif i==75.0:
                        location.append('Jasola Vihar')
                    elif i== 82.0:
                        location.append('Okhla Vihar')
                    elif i==129.0:
                        location.append('Jamia Millia Islamia')
                    elif i==260.0:
                        location.append('Sukhdev Vihar')
                    elif i== 252.0:
                        location.append('Okhla NSIC')
                    elif i==135.0:
                        location.append('Kalkaji Mandir')
                    elif i==205.0:
                        location.append('Nehru Enclape')
                    elif i== 11.0:
                        location.append('Greater Kailash')
                    elif i==243.0:
                        location.append('Chirag Delhi')
                    elif i==174.0:
                        location.append('Panchsheel Park')
                    elif i== 254.0:
                        location.append('Hauz Khas')
                    elif i==242.0:
                        location.append('Malviy Nagar')
                    elif i==36.0:
                        location.append('Saket')
                    elif i== 37.0:
                        location.append('Qutab Minar')
                    elif i==115.0:
                        location.append('Chhattarpur')
                    elif i==156.0:
                        location.append('Sultanpur')
                    elif i== 245.0:
                        location.append('MG Road')  
                    elif i==214.0:
                        location.append('Huda City Center')
                    else:
                        location.append('Not Present')
                return location
        
            a = source_data()
            path="../data/year"
            relative_new_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/csv/"
            df = pd.read_csv(relative_new_path + dataset)
            # df = AddDurationCol(dataset)
            self.dataset_Column_Changes(df)

            df['DOlocation'] =  locationName(df['dolocationid'])
            
            csv_dump_path=path+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/"+"updated_data/location"+"/"
            df.to_csv(csv_dump_path+"updated_"+dataset) 
            
        except Exception as e:
            mail = Mail()
            mail.initiate_email('AddLocationCol',e,0)
            return None

    def dataset_Column_Changes(self,df):
        """This function replace all column names in lowercase and also replace column which name is pickupdatetime and dropoffdatetime """
        lowerCol = [i.lower() for i in list(df.columns)]
        col = df.columns.tolist()
        b = {col[x]: lowerCol[x] for x in range(len(col))}
        df.rename(columns= b, inplace= True)
        col = df.columns.tolist()
        for i in col:
            if "pickup_datetime" in i:
                df.rename(columns= {i:"pickup_datetime"}, inplace= True)
            if "dropoff_datetime" in i:
                df.rename(columns= {i:"dropoff_datetime"}, inplace= True)
            
        return df

    def csvfiles(self):
        """This function collect csv files and transform those"""
        a = source_data()
        relative_new_path="../data/year"+"/"+str(a[0])+"/"+"months"+"/"+str(a[1])+"/csv"
        for file in os.listdir(relative_new_path):
            self.addLocationCol(file)
            self.addDurationCol(file)
        logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logger=logging.getLogger(__name__)
        logger.info("Location_transformation_completed!") 
        logger.info("Duration_transformation_completed!")

        # print("data updated, transfer into folder")



