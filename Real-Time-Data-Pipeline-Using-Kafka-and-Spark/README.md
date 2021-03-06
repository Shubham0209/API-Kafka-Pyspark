# Real-Time Data Pipeline Using Kafka and Spark

## Data Pipeline Architecture

  

![](https://lh4.googleusercontent.com/eZykZAZj43p1oYAZFf_X3CINjHx6qz1rRevNptNWWisXYmDYDEae7Fhla7ETWZ2TmGRvTECBlMtFBe6aKHWaVUac7imu_hOXgVLZwFebuvE-_O_FmSZgdb5kBJAFMAxBl3AAgsYD)

-   ### API
   
	-   The API mimics the water quality sensor data similar to the one shared [here](https://data.world/cityofchicago/beach-water-quality-automated-sensors).
	    
	-   The implementation is done in flask web framework and the response is as follows:
	    

		
-   ### Kafka Producer (Topic: RawSensorData)
    
	
	-   The data from the API stream is pushed to Kafka Producer under topic: RawSensorData
	    


-   ### Apache Spark and Kafka Consumer (Topic: CleanSensorData)
    

	-   The data under the topic RawSensorData is streamed through Kafka Consumer. The data is then structured and validated using Spark.
	    

	  

	-   The cleaned data is then pushed to MongoDB and Kafka Producer under topic: CleanSensorData
    

  

		![](https://lh6.googleusercontent.com/DBMkx3tX90NCtokgNYT4BkjJGujCyeZk08X4w99vo2zfsBN9Yz1YGtb38Tcc3F6_HtMbML9NLVcHPFW310MDSSLWg8G8KoTuo-sC00aApDdNW9ql1ny605pwV6r5DS-Y5D325elU)

-   ### MongoDB
    

	-   The structured data is pushed to MongoDB collection with the following schema:
	    
		```markdown
		| Keys             | Data Type |
		|------------------|-----------|
		| _id              | Object Id |
		| Beach            | String    |
		| MeasurementID    | long      |
		| BatteryLife      | Double    |
		| RawData          | String    |
		| WaterTemperature | Double    |
		| Turbidity        | Double    |
		| TimeStamp        | timestamp |
		```  
  
  

## How to run the code

  

-   #### Start the API (port: 3030)
    

  		 python sensor.py
	    

  

-   #### Start Zookeeper
    

		 bash /opt/zookeeper-3.4.14/bin/zkServer.sh start
    

  

-   #### Start Kafka
    

		bin/kafka-server-start.sh config/server.properties
    

  

-   #### Create RawSensorData Topic
    

		   ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic RawSensorData
    

  

-   #### Create CleanSensorData Topic
    

		 ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic CleanSensorData
    

  

-  #### Push Data From API Stream to Kafka Topic: RawSensorData
    

		python push_data_to_kafka.py
    

  

-   #### Structure and Validate Data, Push To MongoDB and Kafka Topic CleanSensorData
    

		  python structure_validate_store.py


    

  

-  #### View RawSensorData Topic
    

		bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic RawSensorData --from-beginning
    

  

-   #### View CleanSensorData Topic
    

		bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic CleanSensorData --from-beginning
    

  

-   #### Real-Time DashBoard - Visualization
    

		bokeh serve --show dashboard.py
