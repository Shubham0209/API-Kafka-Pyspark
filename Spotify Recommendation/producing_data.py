import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS, api_version=(2, 0, 2),
                                       value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "raw_data.csv"
    
    
    songs_df = pd.read_csv(filepath)
    #songs_df = songs_df[songs_df['added_at'] > '2020-01-01']
    # songs_df = songs_df[songs_df['track_pop'] > 50]
    
    
    songs_df['order_id'] = np.arange(len(songs_df))
    
    #print(songs_df.head(1))
    
    song_list = songs_df.to_dict(orient="records")
    
    
    

    message_list = []
    message = None
    for message in song_list:
        
        message_fields_value_list = []
        
        
        message_fields_value_list.append(message["order_id"])
        message_fields_value_list.append(message["track_id"])
        message_fields_value_list.append(message["track_name"])
        message_fields_value_list.append(message["track_pop"])
        message_fields_value_list.append(message["track_duration"])
        message_fields_value_list.append(message["album"])
        message_fields_value_list.append(message["artist_names"])
        message_fields_value_list.append(message["added_at"])
        message_fields_value_list.append(message["danceability"])
        message_fields_value_list.append(message["energy"])
        message_fields_value_list.append(message["key"])
        message_fields_value_list.append(message["loudness"])
        message_fields_value_list.append(message["speechiness"])
        message_fields_value_list.append(message["acousticness"])
        message_fields_value_list.append(message["instrumentalness"])
        message_fields_value_list.append(message["liveness"])
        message_fields_value_list.append(message["valence"])
        message_fields_value_list.append(message["tempo"])
        message_fields_value_list.append(message["time_signature"])



        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


    print("Kafka Producer Application Completed. ")