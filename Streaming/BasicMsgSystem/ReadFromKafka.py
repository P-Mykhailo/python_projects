from kafka import KafkaConsumer
import json
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('RedditProgramming',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')),
                         auto_offset_reset='earliest', enable_auto_commit=False)

for message in consumer:
    print(message)



