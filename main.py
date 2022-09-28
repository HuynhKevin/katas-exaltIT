from FlightRadar24.api import FlightRadar24API
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import questions
import preprocessing

def main():
    fr_api = FlightRadar24API()
    airports = fr_api.get_airports()
    airlines = fr_api.get_airlines()
    flights = fr_api.get_flights()
    zones = fr_api.get_zones()

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

    countries_continents_df = spark.read.csv('country_continent.csv', header=True)
    countries_continents_df = preprocessing.update_countries_continents(countries_continents_df, spark)

    # Question 1
    print(questions.company_most_flights(flights_df) + " is the company which have the most active flights in the world.")

    # Question 2
    questions.companies_most_regional_flights(flights_df, airports_df, countries_continents_df)

    # Question 3
    longest = questions.longest_route_flight(flights_df, airports_df)
    print("World-wide, the flight with the callsign " + longest[0] + " has the longest route from " + longest[1] + " airport to " + longest[2] + " airport.")

    # Question 4


    # Question 7.1
    questions.airport_most_popular(flights_df, airports_df, countries_continents_df)

    # Question 7.2
    questions.airports_best_balance(flights_df)


main()