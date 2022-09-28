from pyspark.sql.functions import *
from geopy import distance
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Question 1: What is the company with the most active flights in the world ?

def company_most_flights(df):
    nb_companies_flights = df.groupBy('airline_icao').count().sort(desc("count"))
    # We filter according that N/A is not considered as a company
    company_most_flights = nb_companies_flights.where(nb_companies_flights.airline_icao != "N/A").collect()[0]["airline_icao"]
    return company_most_flights



# Question 2: By continent, what are the companies with the most regional active flights (airports of Origin & Destination within the same continent) ?

def companies_most_regional_flights(spark, flights_df, airports_df):
    flights_df.registerTempTable('flights')
    airports_df.registerTempTable('airports')
    #flights_origin_country = spark.sql('select airline_icao, origin_airport_iata, country as origin_country, destination_airport_iata from flights left join airports on flights.origin_airport_iata = airports.iata')
    flights_origin_country = spark.sql('select *, country as origin_country from flights left join airports on flights.origin_airport_iata = airports.iata')
    flights_origin_country.registerTempTable('flights_origin_country')
    #flights_origin_dest_country = spark.sql('select airline_icao, origin_airport_iata, origin_country, destination_airport_iata, country as destination_country from flights_origin_country left join airports on flights_origin_country.destination_airport_iata = airports.iata')
    flights_origin_dest_country = spark.sql('select *, airports.country as destination_country from flights_origin_country left join airports on flights_origin_country.destination_airport_iata = airports.iata')
    flights_origin_dest_country = flights_origin_dest_country.na.drop(subset=["origin_country", "destination_country"])
    flights_origin_dest_country.show(2)
    print(flights_origin_dest_country.count())



# Question 3: World-wide, Which active flight has the longest route ?
@F.udf(returnType=FloatType())
def distance_udf(a, b):
    return distance.distance(a, b).m

def longest_route_flight(flights_df, airports_df):
    flights_df_coord_origin = flights_df.join(airports_df, flights_df.origin_airport_iata == airports_df.iata, "inner")\
                                        .selectExpr("callsign", "origin_airport_iata", "destination_airport_iata", "lat as origin_lat", "lon as origin_lon", "alt as origin_alt")
    flights_df_coord = flights_df_coord_origin.alias("df_coord_origin").join(airports_df, flights_df_coord_origin.destination_airport_iata == airports_df.iata, "inner")\
                    .selectExpr("df_coord_origin.*", "lat as destination_lat", "lon as destination_lon", "alt as destination_alt")
    
    flights_df_coord = flights_df_coord.withColumn('distance2d', distance_udf(F.array("origin_lat", "origin_lon"), F.array("destination_lat", "destination_lon")))
    flights_df_coord = flights_df_coord.withColumn('distance3d', F.sqrt(col("distance2d")**2 + (col("destination_alt") - col("origin_alt"))**2))
    longest_flight = flights_df_coord.sort(desc('distance3d')).collect()[0]
    return longest_flight["callsign"], longest_flight["origin_airport_iata"], longest_flight["destination_airport_iata"]


# Question 4: By continent, what is the average route distance ? (flight localization by airport of origin)



# Question 5.1: Which leading airplane manufacturer has the most active flights in the world ?


# Question 5.2: By continent, what is the most frequent airplane model ? (airplane localization by airport of origin)


# Question 6: By company registration country, what are the tops 3 airplanes model flying ?


# Question 7.1: By continent, what airport is the most popular destination ?
def airport_most_popular(flights_df, airports_df):
    nb_flights_destination = flights_df.filter(flights_df.destination_airport_iata != 'N/A').groupBy('destination_airport_iata').count()
    nb_flights_destination = nb_flights_destination.join(airports_df, nb_flights_destination.destination_airport_iata == airports_df.iata, "inner").sort(desc("count"))
    famous_airport = nb_flights_destination.collect()[0]["name"]
    return famous_airport


# Question 7.2: What airport has the greatest inbound/outbound flights difference ? (positive or negative)
def airports_best_balance(flights_df):
    nb_flights_destination = flights_df.filter(flights_df.destination_airport_iata != 'N/A').groupBy('destination_airport_iata').count().withColumnRenamed("count", "destination_nb_flight")
    nb_flights_origin = flights_df.filter(flights_df.origin_airport_iata != 'N/A').groupBy('origin_airport_iata').count().withColumnRenamed("count", "origin_nb_flight")
    difference_flights_airports = nb_flights_destination.join(nb_flights_origin, nb_flights_destination.destination_airport_iata == nb_flights_origin.origin_airport_iata, "inner")
    difference_flights_airports = difference_flights_airports.withColumn("inbound/outbound", difference_flights_airports.destination_nb_flight - difference_flights_airports.origin_nb_flight)
    best_positive = difference_flights_airports.sort(desc["inbound/outbound"]).collect()[0]["destination_airport_iata"]
    best_negative = difference_flights_airports.sort(asc["inbound/outbound"]).collect()[0]["destination_airport_iata"]
    return best_positive, best_negative


# Question 8: By continent, what is the average active flight speed ? (flight localization by airport of origin)
