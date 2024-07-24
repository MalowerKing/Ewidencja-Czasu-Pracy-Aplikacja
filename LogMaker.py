from email.mime.text import MIMEText 
from email.mime.image import MIMEImage 
from email.mime.application import MIMEApplication 
from email.mime.multipart import MIMEMultipart 
import smtplib
import Credentials
import os

class Log:
    def __init__(self, credentials):
        self.domena = credentials['domena']
        self.port = credentials['port']
        self.login = credentials['login']
        self.password = credentials['password']
        
        
    def message(subject="System Ewidencji Gminy Informacja",  text=""):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg.attach(MIMEText(text))
        return msg
    
    def mail(self):
        print("Mail is making")
        serwer = self.domena
        smtp = smtplib.SMTP_SSL(serwer, timeout=30) 

        print('Open connection')
        smtp.login(self.login, self.password)
        msg = self.message()

        to = "wiktor.f4rys@gmail.com"
        
        smtp.sendmail(from_addr=self.login, to_addrs=to, msg='seks')
        print('sended')
        smtp.quit()

mail = Log(Credentials.Credentials.automaticEmail)
mail.mail()