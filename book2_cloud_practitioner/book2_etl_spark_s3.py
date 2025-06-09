# book2_etl_spark_s3.py
#
# This script is from Chapter 6 of "Cloud Data Engineering for Practitioners."
# It is designed to be run on a cloud service like Amazon EMR.
# It reads data from an S3 bucket and writes the transformed data back to S3.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import sys
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_spark_s3_etl(source_path, destination_path):
    """
    A PySpark ETL job that reads from and writes to S3.
    :param source_path: The S3 path for the source JSON data.
    :param destination_path: The S3 path for the output Parquet data.
    """
    
    # In a real EMR cluster, you don't need to specify a master.
    # The cluster configuration handles it.
    spark = SparkSession.builder \
        .appName("S3-Spark-ETL") \
        .getOrCreate()

    logging.info("Spark Session created successfully.")
    
    try:
        logging.info(f"Reading data from {source_path}...")
        df = spark.read.json(source_path)

        # Transformations remain identical to the local script
        transformed_df = df.select(
            col("id"),
            col("name"),
            col("event_timestamp"),
            col("details.price").alias("price")
        ).where(col("name") == "Special Event")

        transformed_df = transformed_df.withColumn(
            "event_timestamp_dt",
            to_timestamp(col("event_timestamp"))
        )
        
        logging.info(f"Writing transformed data to {destination_path}...")
        transformed_df.write.mode("overwrite").parquet(destination_path)
        
        logging.info("ETL job completed successfully.")

    finally:
        logging.info("Stopping Spark Session.")
        spark.stop()

if __name__ == "__main__":
    # In a real-world scenario, you might get these paths from command-line arguments.
    # For this example, they are hardcoded.
    # IMPORTANT: Replace 'your-unique-bucket-name' with the actual S3 bucket name.
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Local test mode
        BUCKET_NAME = "your-local-test-folder"
        source_s3_path = f"{BUCKET_NAME}/raw-data/source_data.json"
        destination_s3_path = f"{BUCKET_NAME}/processed-data/"
    else:
        # Cloud mode
        BUCKET_NAME = "s3://your-unique-bucket-name"
        source_s3_path = f"{BUCKET_NAME}/raw-data/"
        destination_s3_path = f"{BUCKET_NAME}/processed-data/"
    
    run_spark_s3_etl(source_s3_path, destination_s3_path)
