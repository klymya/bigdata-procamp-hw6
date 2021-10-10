import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, FloatType


INPUT_PATH = "gs://gl-procamp-bigdata-datasets/2015_Flight_Delays_and_Cancellations"
OUTPUT_PATH = "gs://gl-procamp-bigdata-datasets/lab2"

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

    df_flights.registerTempTable("FLIGHTS")
    df_airlines.registerTempTable("AIRLINES")
    df_airports.registerTempTable("AIRPORTS")

    tmp = spark.sql("""
        SELECT f.ORIGIN_AIRPORT, f.AIRLINE, f.DEPARTURE_DELAY, p.AIRPORT as AIRPORT_NAME, a.AIRLINE as AIRLINE_NAME FROM FLIGHTS f 
        INNER JOIN AIRPORTS p ON p.IATA_CODE = f.ORIGIN_AIRPORT 
        INNER JOIN AIRLINES a ON a.IATA_CODE = f.AIRLINE
    """)
    tmp.createOrReplaceTempView("ENRICHED_FLIGHTS")

    tmp = spark.sql("""
        SELECT ORIGIN_AIRPORT, AIRLINE, MAX(AIRPORT_NAME) AS AIRPORT_NAME, MAX(AIRLINE_NAME) AS AIRLINE_NAME, AVG(DEPARTURE_DELAY) AS AVG_AIRLINE_DEPARTURE_DELAY, 
        ROW_NUMBER() OVER (PARTITION BY ORIGIN_AIRPORT ORDER BY AVG(DEPARTURE_DELAY) DESC) AS rn 
        FROM ENRICHED_FLIGHTS 
        GROUP BY ORIGIN_AIRPORT, AIRLINE
    """)
    tmp.createOrReplaceTempView("delays_per_airport_and_airline")

    tmp = spark.sql("""
        SELECT * FROM delays_per_airport_and_airline 
        WHERE rn = 1
    """)
    tmp.createOrReplaceTempView("max_delayer_airline_per_airport")

    tmp = spark.sql("""
        SELECT ORIGIN_AIRPORT, AVG(DEPARTURE_DELAY) AS AVG_DEPARTURE_DELAY, MAX(DEPARTURE_DELAY) AS MAX_DEPARTURE_DELAY 
        FROM FLIGHTS 
        GROUP BY ORIGIN_AIRPORT
    """)
    tmp.createOrReplaceTempView("delays_per_airport")

    df_final_result = spark.sql("""
        SELECT a.ORIGIN_AIRPORT, AIRPORT_NAME, AVG_DEPARTURE_DELAY, MAX_DEPARTURE_DELAY, AIRLINE, AIRLINE_NAME, AVG_AIRLINE_DEPARTURE_DELAY 
        FROM delays_per_airport a 
        INNER JOIN max_delayer_airline_per_airport b ON a.ORIGIN_AIRPORT = b.ORIGIN_AIRPORT
    """)

    df_final_result.coalesce(1).write.mode('overwrite').option("header", "true")\
        .csv(f"{opath}/lab2_results_sql")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_path", default=INPUT_PATH, help="a path to source data.")
    parser.add_argument("-o", "--out_path", default=OUTPUT_PATH, help="a path where save result.")
    args = parser.parse_args()

    main(args.input_path, args.out_path)
