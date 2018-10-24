import syslog
import os
import time
from sys import platform, exit
from syslogger import Syslogger
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import subprocess
import re
import config_onboard

IP_server = config_onboard.IP_server

def get_IP_address():
    output = os.popen("ifconfig eth0").readlines()
    if len(output) != 0:
        IP_info = output[1]
    return IP_info

def test_ping_IP():
        
    p = subprocess.Popen('ping -c 4 '+str(IP_server),
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    output, error = p.communicate()
    res = re.search('unreachable', str(error))
    if res is None:
        success = True
    else:
        success = False
    return success

def send_warning_onshore(content):
    
        recipients = ['ebuchoud@seaspanltd.ca']
        
        FROM = 'master@VRRQ4.seaspanfleet.com'
        SUBJECT = 'ITSS Datacollect Results'

        msg = MIMEMultipart('alternative')
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = ', '.join(recipients)
        IP_info = get_IP_address()
        content = content + str(IP_info)

        body = MIMEText(content)
        msg.attach(body)
        
        part = MIMEBase('application', 'octet-stream')
             
        server = smtplib.SMTP(IP_server)
        server.ehlo()
        server.sendmail(FROM, recipients, msg.as_string())
        server.quit()
        print('Email sent')
        return True


class Uploader(object):

    def __init__(self, localFilePath: str,fileName: str, identifier: int, timestamp: int):
        self.localFilePath = localFilePath
        self.IP_info = get_IP_address()
        self.connect_server = test_ping_IP()

    def send_file_onshore(self, content2, attach):
    
        recipients = ['itss_datacollect@seaspanltd.ca','ebuchoud@seaspanltd.ca']
        content = 'ITSS Datacollect ' + str(content2)
        FROM = 'master@VRRQ4.seaspanfleet.com'
        SUBJECT = 'ITSS Datacollect Results'

        msg = MIMEMultipart('mixed')
        msg['Subject'] = SUBJECT
        msg['From'] = FROM
        msg['To'] = ', '.join(recipients)
        content = content + str(self.IP_info)
        body = MIMEText(content,'plain')
        encoders.encode_quopri(body)
        msg.add_header('Content-Type', 'text')
        msg.attach(body)
        
        part = MIMEBase('application', 'octet-stream; name="%s"' % os.path.basename(attach))
        if attach != '':

            part.set_payload(open(attach, 'rb').read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment')
            msg.attach(part)
            
        server = smtplib.SMTP(IP_server)
        server.ehlo()
        server.sendmail(FROM, recipients, msg.as_string())
        server.quit()
        print('Email sent')
        return True

        #send_file_onshore(localFilePath)
        # if __name__ == '__main__':
#     localpath = os.path.join('/media', 'usb0', 'spu_export.csv')
#     if os.path.exists(localpath) == False:
#         print("invalid file")
#         exit(9)
    
#     uploader = Uploader(localpath, "test", 0, 0)
#     uploader.storeInS3(imo="000000")
