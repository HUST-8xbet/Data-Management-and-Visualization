from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.utils.helpers import load_config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

def create_spark_session():
    config = load_config()
    return SparkSession.builder \
        .appName("CryptoPriceProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    config = load_config()
    db_props = {
        "user": config['database']['user'],
        "password": config['database']['password'],
        "driver": "org.postgresql.Driver"
    }
    try:
        df.write.jdbc(
            url=config['database']['url'],
            table=config['database']['table'],
            mode="append",
            properties=db_props
        )
        logger.warning(f"Batch {epoch_id} written to Postgres")
    except Exception as e:
        logger.error(f"Error writing to DB: {e}")

def main():
    config = load_config()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("instrument", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers']) \
        .option("subscribe", config['kafka']['topic']) \
        .option("startingOffsets", "latest") \
        .option("group.id", "spark-group") \
        .load()

    
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    processed_df = value_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    query = processed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()
    
    logger.warning("Waiting for data to write to Postgres...")
    query.awaitTermination()

if __name__ == "__main__":
    main()