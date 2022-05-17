'''
receive data coming from kafka (producer.py) and insert data into mongodb
'''
# (English): https://stackoverflow.com/questions/35560767/pyspark-streaming-with-kafka-in-pycharm

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

#Mongo DB
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client['RealTimeDB']
collection = db['RealTimeCollection']

def insert_row(x):
    if x is None or len(x)<1:
        return
    data_list=x.split(',')
    collection.insert({
                'timestamp': data_list[0],
                'temp': data_list[1],
                'turbidity': data_list[2],
                'battery': data_list[3],
                'beach': data_list[4],
                'measure_id': data_list[5]
    })

sc=SparkContext(master='local[*]',appName='test')
ssc=StreamingContext(sc,batchDuration=60)
brokers='localhost:9092'
topic='RawSensorData'
kvs=KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list":brokers})
kvs.pprint()
lines=kvs.map(lambda x:'{},{},{},{}'.format(json.loads(x[1])['TimeStamp'],json.loads(x[1])['WaterTemperature'],
                                           json.loads(x[1])['Turbidity'],json.loads(x[1])['BatteryLife'],json.loads(x[1])['Beach'],
                                           json.loads(x[1])['MeasurementID']))
#lines = kvs.map(lambda x: x[1])
#transform = lines.map(lambda tweet: (tweet, int(len(tweet.split())), int(len(tweet))))

lines.foreachRDD(lambda rdd:rdd.foreach(insert_row))
# transform.foreachRDD(lambda rdd:rdd.foreach(insert_row))

''' 
def publishStream(rdd, kafka_producer, topic):
    print("Pushed to topic {}\n".format(topic))
    records = rdd.collect()
    for record in records:
        kafka_producer.send(topic, value=value)

# Iterate over RDD to push records to Kafka. Alternative to line 37 if you want to again push it back to kafka queue
lines.foreachRDD(lambda rdd: publishStream(rdd, kafka_producer, topic2))
'''

ssc.start()
ssc.awaitTermination()