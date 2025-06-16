from pyspark.sql import SparkSession
import re

def list_gcs_files_dates(bucket_name, GCS_client, prefix=None):

    file_names_dates = []
    blobs = GCS_client.list_blobs(bucket_name, prefix=prefix)

    print(f"Listing files in bucket: '{bucket_name}' (prefix: '{prefix if prefix else 'None'}')")
    for blob in blobs:
        match = re.search(r"sales_data_for_(\d{4}-\d{2}-\d{2})\.csv", blob.name)
        if match:    
            date_str = match.group(1)
            file_names_dates.append(date_str)
        else:
            pass
    return file_names_dates


def create_spark_session():
    spark = SparkSession.builder \
            .appName("FlightBookingAnalysis") \
            .config("spark.sql.catalogImplementation", "hive") \
            .getOrCreate()
    return spark

def close_spark_session(spark_object):
    spark_object.stop()