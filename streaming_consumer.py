import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from pyspark.sql.functions import col, from_json


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 --driver-class-path /home/muaz/postgresql-42.3.7.jar --jars /home/muaz/postgresql-42.3.7.jar pyspark-shell'
kafka_topic_name = "mytopic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession.builder.appName("KafkaStreaming ").getOrCreate()

schema = StructType([
        StructField('index', IntegerType(), True),
        StructField('category', StringType(), True),
        StructField('description', StringType(), True),
        StructField('downloaded', IntegerType(), True),
        StructField('follower_count', IntegerType(), True),
        StructField('image_src', StringType(), True),
        StructField('is_image_or_video', StringType(), True),
        StructField('save_location', StringType(), True),
        StructField('tag_list', StringType(), True),
        StructField('title', StringType(), True),
        StructField('unique_id', StringType(), True)
])

stream_df = spark \
        .readStream \
        .format("Kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()

def transformations(dataframe, epoch_id):
        dataframe = dataframe.withColumn('value', col('value').cast(StringType()))
        dataframe = dataframe.withColumn("value", from_json(dataframe["value"], schema)).select(col("value.*"))
        dataframe.write.format('jdbc') \
                .mode('append') \
                .option('url', 'jdbc:postgresql://localhost/pinterest_streaming') \
                .option('dbtable', 'experimental_data') \
                .option('user', 'postgres') \
                .option('password', 'mypassword') \
                .save()


stream_df.writeStream.foreachBatch(transformations).start().awaitTermination()