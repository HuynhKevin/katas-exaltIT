from FlightRadar24.api import FlightRadar24API
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import questions

def main():
    fr_api = FlightRadar24API()
    airports = fr_api.get_airports()
    airlines = fr_api.get_airlines()
    flights = fr_api.get_flights()

    # Create Spark Session
    spark = (
        SparkSession.builder.getOrCreate()
    )

    # Transform data lists into spark dataframe
    airports_schema = StructType([StructField("name", StringType(), True)\
                        ,StructField("iata", StringType(), True)\
                        ,StructField("icao", StringType(), True)\
                        ,StructField("lat", StringType(), True)\
                        ,StructField("lon", StringType(), True)\
                        ,StructField("country", StringType(), True)\
                        ,StructField("alt", StringType(), True)])

    airlines_schema = StructType([StructField("Name", StringType(), True)\
                       ,StructField("Code", StringType(), True)\
                       ,StructField("ICAO", StringType(), True)])

    airports_df = spark.createDataFrame(data = airports, schema=airports_schema)
    airlines_df = spark.createDataFrame(data = airlines, schema = airlines_schema)
    flights_df = spark.createDataFrame(data = flights)

    # Question 1
    print(questions.company_most_flights(flights_df) + " is the company which have the most active flights in the world.")

    # Question 7.1
    print(questions.airport_most_popular(flights_df, airports_df) + " is the most popular destination airport")

    print(flights_df.count())

main()