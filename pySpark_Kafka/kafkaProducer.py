# Standard Library
import csv
import json
from time import sleep

# Third-Party Libraries
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

with open('twitter_validation.csv') as file_obj:
    reader_obj = csv.reader(file_obj)
    for data in reader_obj: 
        producer.send('numtest', value=data)
        sleep(3)