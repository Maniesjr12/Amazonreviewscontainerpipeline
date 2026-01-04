
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    """
    Initializes a SparkSession, connects to PostgreSQL, performs a join and aggregation, 
    and writes the resulting report back to the database.
    """
    
    # 1. Create SparkSession
    # Ensure the PostgreSQL JDBC driver is available on the classpath. 
    # This path assumes the driver was downloaded to /opt/spark/jars/ in the Dockerfile.
    spark: SparkSession = (
        SparkSession.builder
        .appName("PySparkPostgresExample")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.4.jar")
        .getOrCreate()
    )

    print("SparkSession created successfully.")
    
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

    # Helper function to read a table
    def read_pg_table(table_name):
        return (
            spark.read
            .format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", table_name)
            .load()
        )

    # 2. Read orders, users, and products tables
    # NOTE: PySpark column references often require aliases for joins
    orders_df = read_pg_table("orders").alias("o")
    users_df = read_pg_table("users").alias("u")
    products_df = read_pg_table("products").alias("p")
    
    print(f"Loaded orders, users, and products.")

    # 3. Add product info to the orders table (JOIN)
    # The join condition is p.id === o.product_id
    orders_with_price = orders_df.join(
        products_df, 
        F.col("p.id") == F.col("o.product_id"), 
        "inner"
    )
    
    # Cache the result (Equivalent to .cache() in Scala)
    orders_with_price.cache()
    print("Joined DataFrame created and cached.")

    # 4. Get total quantity and money spent per item per day (AGGREGATION)
    
    # We aggregate on created_at, product_id (p.id), and product_name (p.name).
    # We calculate total_spent by multiplying quantity by price before summing.
    report_df = orders_with_price.groupBy(
        F.col("o.created_at").alias("order_date"),
        F.col("p.id").alias("product_id"),
        F.col("p.name").alias("product_name")
    ).agg(
        # Calculate total quantity ordered
        F.sum(F.col("o.quantity")).alias("total_order_quantity"),
        # Calculate total spent (quantity * price)
        F.sum(F.col("o.quantity") * F.col("p.price")).alias("total_order_spent")
    ).withColumn(
        "total_order_spent", F.round(F.col("total_order_spent"), 2)
    ).orderBy("order_date", "product_id")
    
    print("\nReport DataFrame calculated.")

    # 5. Write the final report back to the PostgreSQL database
    # The JDBC properties are passed directly via the options argument
    try:
        (
            report_df.write
            .format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", "report")
            .mode("overwrite") # equivalent to .write.mode("overwrite").jdbc(...)
            .save()
        )
        print("Successfully wrote aggregated report to 'report' table in PostgreSQL.")
    except Exception as e:
        print(f"\nError writing to PostgreSQL 'report' table: {e}")
        
    spark.stop()
    print("Spark Session stopped.")

if __name__ == "__main__":
    main()