from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

KAFKA_IP = '172.17.0.13:9092'
TOPIC_NAME = "raw_transactions_svistunov_wave27"

HDFS_NAMENODE = 'rc1a-dataproc-m-ucdvdhi2gxsxj4y9.mdb.yandexcloud.net:8020'
PATH_TO_HDFS = f'hdfs://{HDFS_NAMENODE}/user/a.svistunov/a.svistunov_wave27/'

def consumer_transactions():
    spark = SparkSession.builder \
        .appName('consumer_transactions_kafka_to_hdfs') \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    try:
        schema = StructType() \
            .add("client_id", StringType()) \
            .add("transaction_id", StringType()) \
            .add("transaction_date", StringType()) \
            .add("transaction_type", StringType()) \
            .add("account_number", StringType()) \
            .add("currency", StringType()) \
            .add("amount", StringType())

        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_IP) \
            .option('subscribe', TOPIC_NAME) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        filtered_df = parsed_df.dropna()
        filtered_df.printSchema()

        checkpoint_path = f"{PATH_TO_HDFS}_checkpoints/{TOPIC_NAME}"

        query = filtered_df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", f"{PATH_TO_HDFS}{TOPIC_NAME}") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(once=True) \
            .option("compression", "snappy") \
            .start()

        query.awaitTermination()

    finally:
        spark.stop()