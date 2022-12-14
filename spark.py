import multiprocessing
import os
import pandas as pd
import pyspark
import json
import ast
import boto3
import shutil
import os

os.environ['PYSPARK_SUBMIT_ARGS']='--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'


def create_downloaded_folder():
        dir = 'downloaded/'
        try:
            shutil.rmtree(dir)
        except:
            pass
        os.mkdir(dir)
        print('downloaded directory created')



def convert_to_dict(i):
    json_file = open(f'downloaded/data_{i}.json')
    json_str = json_file.read()
    json_dict_str = json.loads(json_str)
    json_dict = ast.literal_eval(json_dict_str)
    return json_dict



def convert_follower_count_to_int64(df):
    bad_num = df["follower_count"]
    if "k" in str(bad_num):
        num_str = bad_num.str.replace(r'\D', '000')
    elif 'M' in str(bad_num):
        num_str = bad_num.str.replace(r'\D', '000000')
    num = num_str.astype('int64')
    df["follower_count"] = num



def convert_category_to_category(df):
    bad_cat = df['category']
    cat = bad_cat.astype('category')
    df['category'] = cat



def convert_iv_to_category(df):
    bad_iv = df['is_image_or_video']
    iv = bad_iv.astype('category')
    df['is_image_or_video'] = iv



cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)



def spark():
    session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
    s3 = boto3.client('s3')
    d = {'json_dict' : []}
    for i in range(10):
        s3.download_file('pinterest-data-b11bee4a-d3cb-4ea3-98a9-4be10f13a673', f'msg_{i}_data', f'downloaded/data_{i}.json')
        d['json_dict'].append(convert_to_dict(i))
    df = pd.DataFrame.from_dict(d['json_dict'])
    convert_follower_count_to_int64(df)
    convert_category_to_category(df)
    convert_iv_to_category(df)
    df_spark = session.createDataFrame(df)
    df_spark.show()
    df_spark.write.format('org.apache.spark.sql.cassandra').mode('append').option('spark.cassandra.connection.host', 'localhost:9042') \
        .option('keyspace', 'data') \
        .option('table', 'spark_data').save()

create_downloaded_folder()
spark()