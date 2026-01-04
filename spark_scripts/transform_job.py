# /opt/spark/spark_app/transform_job.py

# Import libraries needed for the data transformation
import os
import sys
import gzip
import json
from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, BooleanType
from pyspark.sql.functions import (
    monotonically_increasing_id, from_unixtime, to_timestamp, col, 
    to_date, date_format, weekofyear
)

# --- Configuration (Hardcoded for simplicity, matches DAG setup) ---
# NOTE: In a real system, use environment variables or Airflow XComs for paths.
# Assuming 'config["storage"]["raw"]' maps to this location:
raw_dir = "/opt/spark/spark_app/data/raw" 
processed_dir = "/opt/spark/spark_app/data/processed"

# Initialize processed staging area, if not exists (redundant with DAG setup, but safe)
os.makedirs(processed_dir, exist_ok=True)


# --- Utility Functions for Reading Raw Data ---

def unzip_files_in_folder(file_name):
    """Unzips and reads a single .json.gz file, adding the 'state' from the filename."""
    data_records = []
    
    # Check if the file exists before opening
    file_path = os.path.join(raw_dir, file_name)
    if not os.path.exists(file_path):
        print(f"Warning: File not found at {file_path}. Skipping.")
        return []
        
    try:
        g = gzip.open(file_path, 'rt', encoding='utf-8')
    except Exception as e:
        print(f"Error opening gzipped file {file_name}: {e}")
        return []

    for i, item in enumerate(g):
        try:
            record = json.loads(item)
            # Extract the federal state of the business from the file_name
            # Example: 'review-Alaska.json.gz' -> 'Alaska'
            state_part = file_name.split('.')[0]
            record['state'] = state_part.split('-')[-1]
            data_records.append(record)
        except json.JSONDecodeError as e:
            # This catches errors if a single line is malformed JSON
            print(f"Skipping malformed line {i + 1} in {file_name}. Error: {e}")
            continue
    return data_records


def parse_jsonl_data(input_folder):
    """Reads all relevant JSON Lines files in the input folder."""
    meta_records = []
    review_records = []
    
    # Use list() around os.listdir to avoid errors if files are modified during iteration
    for file_name in list(os.listdir(input_folder)):
        if file_name.endswith('.json.gz'):
            if 'meta-' in file_name:
                meta_records.append(unzip_files_in_folder(file_name))
            if 'review-' in file_name:
                review_records.append(unzip_files_in_folder(file_name))

    # chain.from_iterable flattens the list of lists into a single generator stream
    return {"review_data": chain.from_iterable(review_records), "meta_data": chain.from_iterable(meta_records)}


# --- MAIN SPARK ETL LOGIC ---

# 1. Initialize Spark Session
print("Initializing Spark Session...")
spark = SparkSession.builder.appName("GoogleReviewETL").getOrCreate()

# 2. Read Raw Data
print("Parsing raw downloaded files...")
raw_data = parse_jsonl_data(raw_dir)


# --- Define Schemas ---

business_schema = StructType([
    StructField('business_name', StringType(), True),
    StructField('address', StringType(), True),
    StructField('url', StringType(), True),
    StructField('gmap_id', StringType(), True),
    StructField('state', StringType(), True),
    StructField('description', StringType(), True),
    StructField('longitude', DoubleType(), True), 
    StructField('latitude', DoubleType(), True),
    StructField('category', StringType(), True),
    StructField('avg_rating', DoubleType(), True), 
    StructField('num_of_reviews', LongType(), True), 
    StructField('price', StringType(), True),
    StructField('hours', ArrayType(ArrayType(StringType())), True)
])

review_schema = StructType([
    StructField('user_id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('gmap_id', StringType(), True),
    StructField('text', StringType(), True),
    StructField('rating', DoubleType(), True), 
    StructField('review_time', LongType(), True), 
    StructField('has_response', BooleanType(), True),
    StructField('has_picture', BooleanType(), True)
])

# 3. Process Business Metadata
print("Creating Business Metadata DataFrame...")
business_data = []
for metadata in raw_data['meta_data']:
    # Your original dict creation logic goes here (simplified for brevity)
    business_data.append({
        'business_name': metadata.get('name'),
        'description': metadata.get('description'),
        'address': metadata.get('address'),
        'url': metadata.get('url'),
        'gmap_id': metadata.get('gmap_id'),
        'state': metadata.get('state'),
        'longitude': float(metadata.get('longitude')) if metadata.get('longitude') else None,
        'latitude': float(metadata.get('latitude')) if metadata.get('latitude') else None,
        'category': ', '.join(metadata.get('category')) if metadata.get('category') else None,
        'avg_rating': float(metadata.get('avg_rating')) if metadata.get('avg_rating') else None,
        'num_of_reviews': metadata.get('num_of_reviews'),
        'price': metadata.get('price'),
        'hours': metadata.get('hours')
    })
df_business_metadata = spark.createDataFrame(business_data, schema=business_schema)

# 4. Process Review Data
print("Creating Review Data DataFrame...")
review_data = []
for review in raw_data['review_data']:
    response = review.get('resp')
    has_response = (isinstance(response, dict) and bool(response.get('text')))
    
    review_data.append({
        'user_id': review.get('user_id'),
        'name': review.get('name'),
        'text': review.get('text'),
        'gmap_id': review.get('gmap_id'),
        'rating': float(review.get('rating')) if review.get('rating') else None,
        'review_time': review.get('time'),
        'has_response': has_response,
        'has_picture': True if review.get('pics') else False
    })

df_review = spark.createDataFrame(review_data, schema=review_schema)


# 5. Dimensional Modeling (PySpark Logic)
print("Starting Dimensional Modeling...")

# Dim_User
dim_user = df_review.select('user_id', 'name') \
    .distinct() \
    .withColumn('user_sk', monotonically_increasing_id()) \
    .select(
        col('user_sk'),
        col('user_id'),
        col('name').alias('reviewer_name')
    )

# Dim_Business
dim_business = df_business_metadata.select(
    'gmap_id', 'business_name', 'description', 'address', 'url', 'longitude', 
    'latitude', 'category', 'price', 'hours', 'state'
).distinct() \
    .withColumn('business_sk', monotonically_increasing_id()) \
    .select(
        col('gmap_id').alias('business_gmap_id'),
        '*'
    ).drop('gmap_id')

# Dim_Date
dim_date_raw = df_review.select('review_time').distinct() \
    .withColumn(
        'review_datetime', 
        from_unixtime((col('review_time') / 1000).cast('long'))
    )

dim_date = dim_date_raw.select(
    col('review_time').alias('date_id'), 
    col('review_datetime').cast('timestamp').alias('full_timestamp'),
    to_date(col('review_datetime')).alias('review_date'), 
    weekofyear(col('review_datetime')).alias('week_of_year'),
    date_format(col('review_datetime'), 'yyyy').cast('int').alias('year'),
    date_format(col('review_datetime'), 'MM').cast('int').alias('month'),
    date_format(col('review_datetime'), 'dd').cast('int').alias('day_of_month'),
    date_format(col('review_datetime'), 'EEEE').alias('day_of_week_name'), 
    date_format(col('review_datetime'), 'HH').cast('int').alias('hour')
).distinct()

# Fact_Review
fact_review_base = df_review.select(
    col('gmap_id'), col('user_id'), col('review_time'), 
    col('text'), col('rating'), col('has_response'), col('has_picture')
)

fact_review_with_business_sk = fact_review_base.join(
    dim_business.select('business_sk', col('business_gmap_id').alias('gmap_id')), 
    on='gmap_id', 
    how='left'
).drop('gmap_id')

fact_review = fact_review_with_business_sk.select(
    col('user_id'), 
    col('business_sk'),
    col('review_time').alias('date_id'), 
    col('rating'), 
    col('has_response'),
    col('has_picture'),
    col('text').alias('review_text')
)

# --- Database Connection Parameters ---
PG_URL = "jdbc:postgresql://postgres/postgres"
PG_USER = "user"
PG_PASSWORD = "password"
PG_DRIVER = "org.postgresql.Driver"

# Base JDBC options used for both reading and writing
jdbc_options = {
    "url": PG_URL,
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": PG_DRIVER
}


# Save Transformed Data (Write to Parquet)

dim_user.write \
.format("jdbc") \
.options(**jdbc_options) \
.option("dbtable", "dim_user") \
.mode("overwrite") \
.save()

dim_date.write \
.format("jdbc") \
.options(**jdbc_options) \
.option("dbtable", "dim_date") \
.mode("overwrite") \
.save()

dim_business.write \
.format("jdbc") \
.options(**jdbc_options) \
.option("dbtable", "dim_business") \
.mode("overwrite") \
.save()

fact_review.write \
.format("jdbc") \
.options(**jdbc_options) \
.option("dbtable", "fact_review") \
.mode("overwrite") \
.save()

# print(f"Saving transformed data to {processed_dir}...")
# dim_user.write.mode("overwrite").parquet(os.path.join(processed_dir, "dim_user"))
# dim_date.write.mode("overwrite").parquet(os.path.join(processed_dir, "dim_date"))
# dim_business.write.mode("overwrite").parquet(os.path.join(processed_dir, "dim_business"))
# fact_review.write.mode("overwrite").parquet(os.path.join(processed_dir, "fact_review"))

print("Spark job finished successfully.")
spark.stop() # Good practice to explicitly stop the Spark session