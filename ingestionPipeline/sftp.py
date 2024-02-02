from importlib.resources import path
import paramiko
import os
import logging
import json

class sftp_connection:    
    """This class create a connection to remote server with sftp and paramiko"""
    def data_from_sftp(self):
        """This function upload all files remote path to local path"""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='localhost',username='annu',password='welcome@123',port=22)
        sftp_client = ssh.open_sftp()

        with open('path.json') as f:
            data = json.load(f)

        for remotepath,localpat in data.items():
            sftp_client.get(remotepath,localpat)

        logging.basicConfig(filename="./Logs/logfile.log",filemode="a",level=logging.DEBUG,format="%(asctime)s %(levelname)s %(name)s %(message)s")
        logger=logging.getLogger(__name__)
        logger.info("parquet file stored")

    
        sftp_client.close()
        ssh.close()



# obj = sftp_connection()
# obj.data_from_sftp()

# relative_new_path="../data/year/2022/months/january/parquet/"

        # for file in os.listdir(relative_new_path):

        #     sftp_client.put(relative_new_path+file,f"/home/annu/test-sftp/{file}")

        # remote_path = "/home/anuradha/test-sftp"
        # for file in os.listdir(remote_path):
        
        # sftp_client.get("","")
        # sftp_client.get("","../data/year/2022/months/january/parquet/fhv_tripdata_2022-01.parquet")
        # sftp_client.get("","../data/year/2022/months/january/parquet/green_tripdata_2022-01.parquet")
        # sftp_client.get("","../data/year/2022/months/january/parquet/yellow_tripdata_2022-01.parquet")