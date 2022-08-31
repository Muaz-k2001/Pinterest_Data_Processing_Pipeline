from kafka import KafkaConsumer
import time
import boto3
import os
import shutil
import json

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
bucket = s3.Bucket('pinterest-data-b11bee4a-d3cb-4ea3-98a9-4be10f13a673')

msg_list = []

def create_raw_data_folder():
        '''Deletes existing raw_data folder and generates new one
        '''
        dir = './raw_data/'
        try:
            shutil.rmtree(dir)
        except:
            pass
        os.mkdir(dir)
        print('raw_data directory created')


def consume():
    consumer = KafkaConsumer('mytopic', group_id='batch', bootstrap_servers=['localhost:9092'])
    a = 0
    for message in consumer:
        msg = message.value.decode('utf-8')
        print(msg)
        msg_list.append(msg)
        a += 1
        if a == 10:
            break
        time.sleep(3)


def create_json_files():
    x = 0
    while x < len(msg_list):
        with open(os.path.join('./raw_data/', f'msg_{x}_data.json'), 'a+') as outfile:
            json.dump(msg_list[x], outfile)
        s3_client.upload_file(f'./raw_data/msg_{x}_data.json', 'pinterest-data-b11bee4a-d3cb-4ea3-98a9-4be10f13a673', f'msg_{x}_data')
        x += 1

create_raw_data_folder()
consume()
create_json_files()