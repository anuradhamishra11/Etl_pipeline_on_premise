from datetime import datetime
from random import randint
import smtplib, ssl
from connect import connect

class Mail:

    
    def __init__(self):

        self.email_config()
        self.port = self.email_config_data[0]
        self.smtp_server_domain_name = self.email_config_data[1]
        self.sender_mail = self.email_config_data[2]
        self.password = self.email_config_data[3]
    

    #def send(self, emails, subject, content):
    def send(self, emails, subject, content):
        ssl_context = ssl.create_default_context()
        service = smtplib.SMTP_SSL(self.smtp_server_domain_name, self.port, context=ssl_context)
        service.login(self.sender_mail, self.password)
        
        for email in emails:
            result = service.sendmail(self.sender_mail, email, f"Subject: {subject}\n{content}")

        service.quit()

    def initiate_email(self,sub,body,status):
        conn_temp = connect()
        
        cursor = conn_temp[0].cursor()
        cursor.execute('SELECT email_to from notification_info where status = 1')
        mails = cursor.fetchall()
        self.send(mails,sub,body)
        curr_dt = datetime.now()
        if conn_temp[1]=="postgresql":
            if status==0:
                cursor.execute("INSERT INTO runlog (jobName,startDateTime,endDateTime,status,error_log) VALUES(%s, %s, %s, %s, %s)", (sub, curr_dt, curr_dt, 'failure', str(body)))
            elif status==1:
                cursor.execute("INSERT INTO runlog (jobName,startDateTime,endDateTime,status,error_log) VALUES(%s, %s, %s, %s, %s)", (sub, curr_dt, curr_dt, 'success', str(body)))
        elif conn_temp[1]=="mssql":
            if status==0:
                cursor.execute("INSERT INTO runlog (jobName,startDateTime,endDateTime,status,error_log) VALUES(?, ?, ?, ?, ?)", (sub, curr_dt, curr_dt, 'failure', str(body)))
            elif status==1:
                cursor.execute("INSERT INTO runlog (jobName,startDateTime,endDateTime,status,error_log) VALUES(?, ?, ?, ?, ?)", (sub, curr_dt, curr_dt, 'success', str(body)))
        conn_temp[0].commit()

    def email_config(self):

        conn_temp = connect()
        cursor = conn_temp[0].cursor()
        cursor.execute('SELECT port, smtp_server_domain_name, sender_mail, password from app_config')

        self.email_config_data = cursor.fetchone()
'''
if __name__ == '__main__':
    mails = input("Enter emails: ").split()
    subject = input("Enter subject: ")
    content = input("Enter content: ")

    mail = Mail()
    mail.send(mails, subject, content)
    #mail.initiate_email()
'''