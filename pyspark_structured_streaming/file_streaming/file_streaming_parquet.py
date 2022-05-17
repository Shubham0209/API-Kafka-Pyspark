from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Streaming Application Started ...")

    spark = SparkSession\
            .builder\
            .appName("File Streaming Application - Parquet")\
            .master("local[*]")\
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

    #spark.sparkContext.setLogLevel('ERROR')

    # registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
    input_parquet_schema = StructType([
        StructField("registration_dttm", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("cc", StringType(), True),
        StructField("country", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("comments", StringType(), True)
    ])

    # .schema(input_parquet_schema) \
    stream_df = spark\
                .readStream\
                .format("parquet") \
                .option("path","input_data/parquet")\
                .load()

    print(stream_df.isStreaming)
    print(stream_df.printSchema())

    write_stream_query = stream_df\
        .writeStream\
        .outputMode("update")\
        .format("console")\
        .option("checkpointLocation", "streaming-checkpoint-loc-parquet")\
        .trigger(processingTime="10 second")\
        .start()

    write_stream_query.awaitTermination()

    print("Streaming Application Completed.")
