import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Cấu hình logging để xem lỗi cho dễ
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoPriceProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Chỉ hiện lỗi quan trọng, đỡ rác màn hình

    logger.info("--- ĐANG KHỞI ĐỘNG SPARK STREAMING ---")

    # 1. Định nghĩa cấu trúc dữ liệu (Schema) giống hệt cái JSON bên Airflow gửi
    schema = StructType([
        StructField("instrument", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # 2. Đọc dữ liệu từ Kafka
    # Lưu ý: Dùng cổng 29092 (cổng nội bộ Docker) mà mình vừa sửa lúc nãy
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "bitcoin_prices") \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Chuyển đổi dữ liệu (từ dạng Binary sang String rồi sang JSON)
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 4. In kết quả ra màn hình (Console) để kiểm tra
    query = value_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    logger.info("--- ĐANG CHỜ DỮ LIỆU TỪ KAFKA... ---")
    query.awaitTermination()

if __name__ == "__main__":
    main()