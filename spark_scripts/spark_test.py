# spark_job.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create the SparkSession
    spark = SparkSession.builder \
        .appName("SimpleSparkJob") \
        .getOrCreate()

    # Read the text file (the script itself) and count lines
    log_data = spark.sparkContext.textFile("spark_job.py")
    num_a = log_data.filter(lambda s: 'a' in s).count()
    num_b = log_data.filter(lambda s: 'b' in s).count()

    print("Successfully ran PySpark job!")
    print(f"Lines with 'a': {num_a}")
    print(f"Lines with 'b': {num_b}")

    spark.stop()