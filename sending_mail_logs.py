import os
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
today=datetime.today().strftime('%Y-%m-%d')
import glob
import pandas as pd

class Gmail:
    def __init__(self, subject=None, from_=None, to=None, password=None):
        self.subject = subject
        self.from_ = from_
        self.to = to
        self.msg = MIMEMultipart()
        self.msg['Subject'] = subject
        self.msg['From'] = from_
        self.msg['To'] = to
        self.password = password

    def attach_message(self, message=None, html_file_path=None):
        if message:
            text = MIMEText(message)
            self.msg.attach(text)
        if html_file_path:
            with open(html_file_path, 'r') as f:
                html = f.read()
                text = MIMEText(html, 'html')
                self.msg.attach(text)

    def attach_image(self, img_file_path):
        img_data = open(img_file_path, 'rb').read()
        image = MIMEImage(img_data, name=os.path.basename(img_file_path))
        self.msg.attach(image)

    def attach_file(self, file_path):
        # open the file to be sent
        filename = file_path
        attachment = open(file_path, "rb")

        # instance of MIMEBase and named as p
        p = MIMEBase('application', 'octet-stream')

        # To change the payload into encoded form
        p.set_payload((attachment).read())

        # encode into base64
        encoders.encode_base64(p)

        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        # attach the instance 'p' to instance 'msg'
        self.msg.attach(p)

    def send_mail(self):
        try:
            s = smtplib.SMTP('smtp.gmail.com', 587)
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(self.from_, self.password)
            s.sendmail(self.from_, self.to, self.msg.as_string())
            print("Mail send successfully.")
            s.quit()

        except Exception as e:
            print('Mail failed.')
            print('Found error: ', e)


gmail = Gmail(from_='karunakarammolla@gmail.com', to='karunakarammolla@gmail.com,ullas.kumar@acuitykp.com', password='Ammolla@143',
              subject='wallmart weekly status')

temp = 'C:\\Desktop\\ankiacrape\\output\\'







list_of_files = glob.glob(temp + str(today)+'*.csv')
if len(list_of_files)>0:
    df=pd.read_csv(list_of_files[0],encoding = 'unicode_escape')
    # print(df)
    df.to_html('C:\\Desktop\\ankiacrape\\mahes\\'+str(today)+'page.html')


gmail.attach_message(html_file_path='C:\\Desktop\\ankiacrape\\mahes\\'+str(today)+'page.html',message='Hi team, the following table contains information about Wallmart weekly extracted data. this tables contains coumn wise missing values,total records and total duplicate records')
# gmail.attach_file(file_path=r'C:\Desktop\ankiacrape\output\2020-05-20.csv')#csv
gmail.send_mail()