import time
import requests
from kafka import KafkaProducer
from websocket import create_connection


def get_sensor_data_stream():
    try:
        url = 'http://localhost:3030/sensordata'
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    msg =  get_sensor_data_stream()
    print('message', msg)
    producer.send("RawSensorData", msg.encode('utf-8'))
    time.sleep(1)


    

