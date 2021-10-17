import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, ByteType, LongType, DoubleType
from pyspark.sql.functions import col, count, avg, from_json, to_timestamp, window
from pyspark.sql.functions import sum as pyspark_sum

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "bitstamp.btc.transactions"
OUTPUT_PATH = "gs://gl-procamp-bigdata-datasets/lab3"


SCHEMA_DATA = StructType(fields=[
    StructField("id", LongType(), False),
    StructField("id_str", StringType(), False),
    StructField("order_type", ByteType(), False),
    StructField("datetime", StringType(), False),
    StructField("microtimestamp", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("amount_str", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("price_str", StringType(), False),
])

SCHEMA = StructType(fields=[
    StructField("data", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("event", StringType(), False),
])


def main(server, topic, opath):
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_hw')\
        .getOrCreate()

    df_raw = spark \
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", server)\
        .option("subscribe", topic)\
        .option("startingOffsets", "earliest")\
        .load()

    df_transactions = df_raw.selectExpr("CAST(value AS STRING)")
    df_transactions = df_transactions.filter(col("value").contains("id"))

    df_transactions = df_transactions.withColumn("value", from_json(col("value"), SCHEMA))\
        .select(col("value.data"))\
        .withColumn('data', from_json(col('data'), SCHEMA_DATA)).select(col('data.*'))\

    df_transactions = df_transactions.withColumn("datetime", to_timestamp(col("datetime").cast(LongType())))

    df_res = df_transactions.select('amount', 'price', 'datetime')\
        .withWatermark("datetime", "3 minutes")\
        .groupby(window(col('datetime'), "1 minutes", "1 minutes"))\
        .agg(
            count(col('*')).alias('count_of_records'),
            avg(col('price')).alias('average_price'),
            (pyspark_sum('price') * pyspark_sum('amount')).alias('sales_total')
        )

    query = df_res \
        .writeStream \
        .trigger(processingTime='1 minutes')\
        .format("parquet")\
        .option("checkpointLocation", f"{opath}/checkpoint")\
        .option("path", f"{opath}/data")\
        .start()
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server", default=KAFKA_SERVER, help="an address of kafka server.")
    parser.add_argument("-t", "--topic", default=KAFKA_TOPIC, help="a name of kafka topic.")
    parser.add_argument("-o", "--out_path", default=OUTPUT_PATH, help="a path where save result.")
    args = parser.parse_args()

    main(args.server, args.topic, args.out_path)
