import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

# Cấu hình Database Postgres
DB_URL = "jdbc:postgresql://postgres-dw:5432/crypto_dw"
DB_PROPERTIES = {
    "user": "dw_user",
    "password": "dw_password",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    # Thêm gói org.postgresql:postgresql:42.6.0 để Spark có Driver kết nối DB
    return SparkSession.builder \
        .appName("CryptoPriceProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

# Hàm này sẽ được gọi mỗi khi có một batch dữ liệu mới
def write_to_postgres(df, epoch_id):
    try:
        # Ghi dữ liệu vào bảng 'bitcoin_prices' trong Postgres
        # Mode 'append': Ghi nối tiếp
        df.write.jdbc(url=DB_URL, table="bitcoin_prices", mode="append", properties=DB_PROPERTIES)
        logger.warning(f"--- DA GHI BATCH {epoch_id} VAO POSTGRES ---")
    except Exception as e:
        logger.error(f"Loi ghi Database: {str(e)}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema đọc từ Kafka (Cứ để String trước)
    schema = StructType([
        StructField("instrument", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True) 
    ])

    # Đọc Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bitcoin_prices") \
        .option("startingOffsets", "latest") \
        .load()

    # Xử lý JSON
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    # Ghi vào Postgres bằng foreachBatch
   # Chuyển đổi cột timestamp từ String sang TimestampType chuẩn
    processed_df = value_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    # -----------------------------------------

    # Ghi vào Postgres (Dùng processed_df thay vì value_df)
    query = processed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()

    logger.warning("--- DANG CHO DU LIEU DE GHI VAO POSTGRES... ---")
    query.awaitTermination()
if __name__ == "__main__":
    main()