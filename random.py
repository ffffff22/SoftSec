import pandas as pd
import urllib
import json
import csv
import boto3
from botocore.exceptions import ClientError
import io
import matplotlib.pyplot as plt

url = 'https://bank.gov.ua/NBU_Exchange/exchange_site?start=20210101&end=20211231&valcode=usd&sort=exchangedate&order=asc&json'
url_open = urllib.request.urlopen(url)
out = open("result.json" , 'ab')
out.write(url_open.read())
out.close()

with open('result.json', 'r') as f:
    data = json.load(f)


with open('data.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write the header row
    writer.writerow(data[0].keys())
    
    # Write each data row
    for row in data:
        writer.writerow(row.values())
        
s3_client = boto3.client('s3')
 
 
def upload(bucket, bfile, filename):
        bfile = io.BytesIO(bfile)
        try:
            s3_client.upload_fileobj(bfile, bucket, filename)
        except ClientError as e:
            print(e)
            return False
        return True
 
 

bfile = open("data.csv", "rb").read()
upload("fff-bucket-cloud", bfile, "data.csv")

s3 = boto3.resource('s3')
 
# choose which file to read
obj = s3.Object("x", "data.csv")

# get only the body of the object
file_content = obj.get()['Body'].read().decode('utf-8')
data = pd.read_csv(io.StringIO(file_content))
plt.figure(figsize=(20,8))
plt.plot(data['exchangedate'], data['rate'])
plt.xlabel('Exchange Date')
plt.ylabel('Exchange Rate')
plt.title('USD Exchange Rates')
plt.show()

plt.savefig('task6.png')
 
s3_client = boto3.client("s3")
s3_client.upload_file('task6.png', 'x', 'task6.png')
