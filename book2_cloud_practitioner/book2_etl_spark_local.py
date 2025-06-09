# book2_etl_spark_local.py
#
# This script is from Chapter 4 of "Cloud Data Engineering for Practitioners."
# It demonstrates how to write a simple ETL job using PySpark that reads
# from and writes to the local filesystem.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_spark_etl():
    """A simple ETL job written with PySpark."""
    
    # 1. Create a SparkSession - the entry point to Spark functionality
    # .master("local[*]") tells Spark to run locally using all available CPU cores.
    spark = SparkSession.builder \
        .appName("MyFirstSparkETL") \
        .master("local[*]") \
        .getOrCreate()

    logging.info("Spark Session created successfully.")

    try:
        # 2. EXTRACT: Read data from a source (the source_data.json file)
        logging.info("Extracting data from source_data.json...")
        df = spark.read.json("source_data.json")
        logging.info("Data extracted successfully.")
        
        logging.info("Source Schema:")
        df.printSchema()
        logging.info("Source Data Sample:")
        df.show(5, truncate=False)

        # 3. TRANSFORM: Apply transformations
        logging.info("Transforming data...")
        transformed_df = df.select(
            col("id"),
            col("name"),
            col("event_timestamp"),
            col("details.price").alias("price") # Access nested JSON data
        ).where(col("name") == "Special Event")

        # Change data type for the timestamp
        transformed_df = transformed_df.withColumn(
            "event_timestamp_dt",
            to_timestamp(col("event_timestamp"))
        )
        
        logging.info("Data transformed successfully.")
        logging.info("Transformed Schema:")
        transformed_df.printSchema()
        logging.info("Transformed Data Sample:")
        transformed_df.show(5, truncate=False)

        # 4. LOAD: Write the transformed data to a destination
        # We write to Parquet format, which is highly optimized for analytics.
        # "overwrite" mode will replace any existing data at the destination.
        output_path = "output_data.parquet"
        logging.info(f"Loading transformed data to {output_path}...")
        transformed_df.write.mode("overwrite").parquet(output_path)
        
        logging.info(f"Transformed data loaded successfully to {output_path}.")

    finally:
        # Stop the SparkSession to release resources
        logging.info("Stopping Spark Session.")
        spark.stop()

if __name__ == "__main__":
    run_spark_etl()
