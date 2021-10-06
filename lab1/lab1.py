import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, FloatType
from pyspark.sql.functions import col, asc, row_number, count
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.window import Window


INPUT_PATH = "gs://gl-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations"
OUTPUT_PATH = "gs://gl-procamp-bigdata-datasets/lab1"

SCHEMA_FLIGHT = [
    StructField("YEAR", IntegerType(), False),
    StructField("MONTH", IntegerType(), False),
    StructField("DAY", IntegerType(), False),
    StructField("DAY_OF_WEEK", IntegerType(), False),
    StructField("AIRLINE", StringType(), False),
    StructField("FLIGHT_NUMBER", IntegerType(), False),
    StructField("TAIL_NUMBER", StringType(), False),
    StructField("ORIGIN_AIRPORT", StringType(), False),
    StructField("DESTINATION_AIRPORT", StringType(), False),
    StructField("SCHEDULED_DEPARTURE", IntegerType(), False),
    StructField("DEPARTURE_TIME", IntegerType(), True),
    StructField("DEPARTURE_DELAY", IntegerType(), True),
    StructField("TAXI_OUT", IntegerType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("SCHEDULED_TIME", IntegerType(), True),
    StructField("ELAPSED_TIME", IntegerType(), True),
    StructField("AIR_TIME", IntegerType(), True),
    StructField("DISTANCE", IntegerType(), False),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", IntegerType(), True),
    StructField("SCHEDULED_ARRIVAL", IntegerType(), False),
    StructField("ARRIVAL_TIME", IntegerType(), True),
    StructField("ARRIVAL_DELAY", IntegerType(), True),
    StructField("DIVERTED", IntegerType(), False),
    StructField("CANCELLED", IntegerType(), False),
    StructField("CANCELLATION_REASON", IntegerType(), True),
    StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
    StructField("SECURITY_DELAY", IntegerType(), True),
    StructField("AIRLINE_DELAY", IntegerType(), True),
    StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
    StructField("WEATHER_DELAY", IntegerType(), True)
]
SCHEMA_AIRLINES = [
    StructField("IATA_CODE", StringType(), False),
    StructField("AIRLINE", StringType(), False),
]
SCHEMA_AIRPORTS = [
    StructField("IATA_CODE", StringType(), False),
    StructField("AIRPORT", StringType(), False),
    StructField("CITY", StringType(), False),
    StructField("STATE", StringType(), False),
    StructField("COUNTRY", StringType(), False),
    StructField("LATITUDE", FloatType(), False),
    StructField("LONGITUDE", FloatType(), False),
]

STRUCT_FLIGHT = StructType(fields=SCHEMA_FLIGHT)
STRUCT_AIRLINES = StructType(fields=SCHEMA_AIRLINES)
STRUCT_AIRPORTS = StructType(fields=SCHEMA_AIRPORTS)


def task1(df, opath):
    debug_df = df.groupby('destination.AIRPORT')\
        .pivot('MONTH').count().sort(asc("destination.AIRPORT"))

    windowDept = Window.partitionBy("MONTH").orderBy(col("NUMBER_OF_VISITS").desc())
    res_df = df.select('MONTH', 'destination.AIRPORT')\
        .groupby('MONTH', 'destination.AIRPORT')\
        .agg(count('destination.AIRPORT').alias('NUMBER_OF_VISITS'))\
        .withColumn("row", row_number().over(windowDept))\
        .filter(col("row") == 1).drop("row").orderBy(['MONTH'], ascending=True)

    debug_df.coalesce(1).write.mode('overwrite').option("header", "true")\
        .option("delimiter", "\t").csv(f"{opath}/task1/debug")
    res_df.coalesce(1).write.mode('overwrite').option("header", "true")\
        .option("delimiter", "\t").csv(f"{opath}/task1/result")


def task2(df, opath):
    debug_df = df.groupby('airlines.AIRLINE').agg(count('airlines.AIRLINE').alias('NUMBER_OF_FLIGHTS'))

    res_df = df.groupby('airlines.AIRLINE', 'origin.AIRPORT').agg(
        (count('airlines.AIRLINE') - pyspark_sum('CANCELLED')).alias('NUMBER_OF_PROCESSED_FLIGHTS'),
        pyspark_sum('CANCELLED').alias('NUMBER_OF_CANCELED_FLIGHTS'),
        (pyspark_sum('CANCELLED') / count('airlines.AIRLINE') * 100).alias('PERCENTAGE_OF_CANCELED_FLIGHTS')
    ).orderBy(['airlines.AIRLINE', 'origin.AIRPORT'], ascending=True)

    res_df_json = res_df.filter(col('origin.AIRPORT') != 'Waco Regional Airport')
    res_df_csv = res_df.filter(col('origin.AIRPORT') == 'Waco Regional Airport')

    debug_df.coalesce(1).write.mode('overwrite').option("header", "true")\
        .csv(f"{opath}/task2/debug")
    res_df_csv.coalesce(1).write.mode('overwrite').option("header", "true")\
        .csv(f"{opath}/task2/result_csv")
    res_df_json.coalesce(1).write.mode('overwrite').json(f"{opath}/task2/result_json")


def main(ipath, opath):
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_Tutorial')\
        .getOrCreate()

    df_flights = spark.read.csv(
        f"{ipath}/flights.csv",
        sep=',',
        header=True,
        schema=STRUCT_FLIGHT
    )
    df_airlines = spark.read.csv(
        f"{ipath}/airlines.csv",
        sep=',',
        header=True,
        schema=STRUCT_AIRLINES
    )
    df_airports = spark.read.csv(
        f"{ipath}/airports.csv",
        sep=',',
        header=True,
        schema=STRUCT_AIRPORTS
    )

    # data denormalization to avoid future joins
    df = df_flights.alias('flights')\
        .join(df_airlines.alias('airlines'), df_flights.AIRLINE == df_airlines.IATA_CODE, 'left')\
        .select(['flights.*', 'airlines.AIRLINE'])
    df = df\
        .join(df_airports.alias('origin'), col('flights.ORIGIN_AIRPORT') == col('origin.IATA_CODE'), 'left')\
        .select(['flights.*', 'airlines.*', 'origin.AIRPORT'])
    df = df\
        .join(df_airports.alias('destination'), col('flights.DESTINATION_AIRPORT') == col('destination.IATA_CODE'), 'left')\
        .select(['flights.*', 'airlines.*', 'origin.AIRPORT', 'destination.AIRPORT'])

    task1(df, opath)
    task2(df, opath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", default=INPUT_PATH, help="a path to source data.")
    parser.add_argument("-o", "--out_path", default=OUTPUT_PATH, help="a path where save result.")
    args = parser.parse_args()

    main(args.input_path, args.out_path)
