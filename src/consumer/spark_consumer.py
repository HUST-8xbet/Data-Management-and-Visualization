from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from src.utils.helpers import load_config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CryptoPriceProcessor")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")

        # ===== MinIO (S3A) =====
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ===== S3A COMMITTER (QUAN TRỌNG) =====
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
        .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/s3a")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.speculation", "false")

        .getOrCreate()
    )

def write_to_postgres(df):
    config = load_config()
    df.write.jdbc(
        url=config["database"]["url"],
        table=config["database"]["table"],
        mode="append",
        properties={
            "user": config["database"]["user"],
            "password": config["database"]["password"],
            "driver": "org.postgresql.Driver",
        }
    )

def write_to_minio(df):
    (
        df.write
        .mode("append")
        .partitionBy("date")
        .json("s3a://crypto-raw/")
    )

def write_batch(df, batch_id):
    try:
        if df.rdd.isEmpty():  # Check DF rỗng để skip write
            logger.warning(f"Batch {batch_id} is empty, skipping write")
            return

        # Debug: Show data trước write
        logger.info(f"Batch {batch_id} data preview:")
        df.show(5, truncate=False)  # Show 5 rows để debug

        df_postgres = df.drop("date")
        df_minio = df

        write_to_minio(df_minio)  # Write MinIO trước (thô)
        write_to_postgres(df_postgres)  # Sau đó Postgres (clean)

        logger.warning(f"Batch {batch_id} written to Postgres + MinIO")
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}")
        raise


def main():
    config = load_config()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema cho từng item trong array
    item_schema = StructType([
        StructField("instrument", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", StringType())
    ])

    # Schema cho toàn bộ value: ArrayType của item_schema
    schema = ArrayType(item_schema)

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("subscribe", config["kafka"]["topic"])
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json")
        # Debug: Log raw JSON để check
        .withColumn("raw_json", col("json"))  # Giữ để show trong batch nếu cần
        .select(from_json(col("json"), schema).alias("data"))
        # Explode array thành rows riêng
        .select(explode("data").alias("item"))
        .select("item.*")
        # Fix parse timestamp với format đúng
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("date", to_date("timestamp"))
    )

    query = (
        parsed_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "s3a://crypto-raw/_checkpoints")
        .start()
    )

    logger.warning("Streaming Kafka → Postgres + MinIO started")
    query.awaitTermination()

if __name__ == "__main__":
    main()
