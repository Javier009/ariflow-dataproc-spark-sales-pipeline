from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
# from pyspark.sql.types import DateType, DoubleType, IntegerType
import os
import sys

from google.cloud import storage

#Project id 
PROJECT_ID = 'airflow-dataproc-project'

# GCS variables
RAW_DATA_BUCKET = 'airflow-p1-sales-data'
ARCHIVE_BUCKET = 'airflow-p1-sales-data-archive'

# Big Query Variables
BQ_DATA_SET_ID = 'product_reports'
BQ_TABLE =  'daily_sales'


def move_files_from_bucket(GCS_client, src_bucket_name: str, dst_bucket_name: str, blob_name: str):
    src_bucket = GCS_client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(blob_name)
    dst_bucket = GCS_client.bucket(dst_bucket_name)
    dst_bucket.copy_blob(src_blob, dst_bucket, blob_name)
    src_blob.delete()

def process_data():

    GCS_Client = storage.Client(project=PROJECT_ID)
    os.environ["SPARK_VERSION"] = "3.5"
    spark = SparkSession.builder.appName("Process-Sales-Data").getOrCreate()
    args = sys.argv
    
    gcs_uri = 'gs://airflow-p1-sales-data/'
    files_args = args[1]
    file_list = files_args.split(',')

    chars_to_remove = ['[', ']', "'", " "]

    if file_list == ['[]']:
        print('No new files to process ...')
    else: 
        for file in file_list:

            for c in  chars_to_remove:
                file = file.replace(c, '')
            file_path = f'{gcs_uri}{file}'

            df = spark.read.csv(file_path, header=True, inferSchema=True)\
            .withColumn('total_ammount_discounted', col('units_sold') * col('unit_price') * col('discount_applied') * -1) \
            .withColumn('gross_revenue', col('units_sold') * col('unit_price')) \
            .groupBy(['transaction_date', 'order_country', 'product_id']) \
            .agg(
                sum('units_sold').alias('total_units_sold'),
                sum('total_ammount_paid').alias('daily_total_paid'),
                sum('total_ammount_discounted').alias('daily_total_discount_amount'),
                sum('gross_revenue').alias('daily_gross_revenue')
                )\
            .dropna(how='any')
        
            # Write to BQ
            df.write.format("bigquery")\
                .option("table", f"{PROJECT_ID}.{BQ_DATA_SET_ID}.{BQ_TABLE}")\
                .option("temporaryGcsBucket", "dataproc-staging-us-central1-589100790055-s1mquucs") \
                .mode("append").save()
            
            #  Move file to Archive
            move_files_from_bucket(GCS_Client,
                                src_bucket_name=RAW_DATA_BUCKET, 
                                dst_bucket_name = ARCHIVE_BUCKET,
                                blob_name=file
                                )
            
            print(f'Succesfully processed {file_path}')

if __name__ == "__main__":
    process_data()

# gsutil cp read_and_process_sales_data.py gs://airflow-p1-source-code/read_and_process_sales_data.py



