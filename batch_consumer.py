from kafka import KafkaConsumer
import time


consumer = KafkaConsumer('mytopic', group_id='batch', bootstrap_servers=['localhost:9092'])
for message in consumer:
    message.value.decode('utf-8')
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    time.sleep(3)
