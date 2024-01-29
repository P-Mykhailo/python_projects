from kafka import KafkaProducer
from kafka import KafkaConsumer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
value_serializer=lambda m: json.dumps(m).encode('ascii'))

consumer = KafkaConsumer('RedditProgramming',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('ascii')),
                         auto_offset_reset='earliest', enable_auto_commit=False)

number = 1

for msg in consumer:
    transaction = {
        'id': msg.value.get('id'),
        'comment': f"My comment is {number}"
    }
    producer.send('RedditProgrammingWithComments', transaction)
    print(transaction)
    number += 1





