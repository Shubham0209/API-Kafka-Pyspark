from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
            .builder \
            .appName("Read CSV Data - Batch") \
            .master("local[*]") \
            .getOrCreate()

    batch_df = spark \
                .read \
                .format("csv") \
                .option("header", "true") \
                .load(path="input_data/csv")

    batch_df.show(10, False)

    print("Application Completed.")
