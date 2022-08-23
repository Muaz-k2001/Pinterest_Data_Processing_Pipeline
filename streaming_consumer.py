from kafka import KafkaConsumer


consumer = KafkaConsumer('MyFirstKafkaTopic', group_id='KafkaGroup', bootstrap_servers=['localhost:9092'])
for message in consumer:
    message.value.decode('utf-8')
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
