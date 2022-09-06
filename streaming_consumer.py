import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'
kafka_topic_name = "mytopic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession.builder.appName("KafkaStreaming ").getOrCreate()


stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()


stream_df.writeStream.format("console").outputMode("append").start().awaitTermination()