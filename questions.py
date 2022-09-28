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

def companies_most_regional_flights(flights_df, airports_df, countries_continents_df):
    flights_df_na = flights_df.filter((flights_df.destination_airport_iata != 'N/A') & (flights_df.origin_airport_iata != 'N/A') & (flights_df.airline_icao != 'N/A'))
    flights_origin_country = flights_df_na.join(airports_df, flights_df_na.origin_airport_iata == airports_df.iata, "inner") \
                                        .selectExpr("airline_icao", "origin_airport_iata", "country as origin_country", "destination_airport_iata")

    flights_origin_dest_country = flights_origin_country.alias("flights_origin_country").join(airports_df, flights_origin_country.destination_airport_iata == airports_df.iata, "inner") \
                                        .selectExpr("flights_origin_country.*", "country as destination_country")

    flights_origin_continent = flights_origin_dest_country.alias("df1").join(countries_continents_df, flights_origin_dest_country.origin_country == countries_continents_df.country, "left")\
                        .selectExpr("df1.*", "continent as origin_continent")
    flights_origin_dest_continent = flights_origin_continent.alias("df2").join(countries_continents_df, flights_origin_continent.destination_country == countries_continents_df.country, "left")\
                        .selectExpr("df2.*", "continent as destination_continent")
    flights_same_continent = flights_origin_dest_continent.filter(flights_origin_dest_continent.origin_continent == flights_origin_dest_continent.destination_continent)
    company_regional_count = flights_same_continent.groupBy(['airline_icao', 'origin_continent']).count()
    continents = ['Europe', 'Asia', 'North America', 'South America', 'Africa', 'Oceania']
    for continent in continents:
        company_most_flight = company_regional_count.filter(company_regional_count.origin_continent == continent).sort(desc("count")).collect()[0]
        print("In " + continent + ", the company: " + company_most_flight['airline_icao'] + " has the most regional active flights with " + str(company_most_flight["count"]) + " regional flights.")



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
def airport_most_popular(flights_df, airports_df, countries_continents_df):
    a = flights_df.filter(flights_df.destination_airport_iata != 'N/A')
    a_destination_country = a.join(airports_df, a.destination_airport_iata == airports_df.iata, "inner").selectExpr("destination_airport_iata", "name as airport_name", "country as airport_country")
    a_destination_continent = a_destination_country.alias("df1").join(countries_continents_df, a_destination_country.airport_country == countries_continents_df.country, "left")\
                        .selectExpr("df1.*", "continent as airport_continent")
    a_destination_count = a_destination_continent.groupBy(["airport_name", "airport_continent"]).count()
    continents = ['Europe', 'Asia', 'North America', 'South America', 'Africa', 'Oceania']
    for continent in continents:
        popular_destination = a_destination_count.filter(a_destination_count.airport_continent == continent).sort(desc("count")).collect()[0]
        print("In " + continent + ", the airport: " + popular_destination["airport_name"] + " is the most popular destination with " + str(popular_destination["count"]) + " flights.")

# Question 7.2: What airport has the greatest inbound/outbound flights difference ? (positive or negative)
def airports_best_balance(flights_df):
    nb_flights_destination = flights_df.filter(flights_df.destination_airport_iata != 'N/A').groupBy('destination_airport_iata').count().withColumnRenamed("count", "destination_nb_flight")
    nb_flights_origin = flights_df.filter(flights_df.origin_airport_iata != 'N/A').groupBy('origin_airport_iata').count().withColumnRenamed("count", "origin_nb_flight")
    difference_flights_airports = nb_flights_destination.join(nb_flights_origin, nb_flights_destination.destination_airport_iata == nb_flights_origin.origin_airport_iata, "inner")
    difference_flights_airports = difference_flights_airports.withColumn("inbound/outbound", difference_flights_airports.destination_nb_flight - difference_flights_airports.origin_nb_flight)
    best_positive = difference_flights_airports.sort(desc("inbound/outbound")).collect()[0]
    best_negative = difference_flights_airports.sort(asc("inbound/outbound")).collect()[0]
    print(best_positive["destination_airport_iata"] + " is the airport with the greatest inbound/outbound flights difference positively with " \
            + str(best_positive["destination_nb_flight"]) + " flights inbound and " + str(best_positive["origin_nb_flight"]) + " flights outbound. ") 
    print(best_negative["destination_airport_iata"] + " is the airport with the greatest inbound/outbound flights difference negatively with " \
            + str(best_negative["destination_nb_flight"]) + " flights inbound and " + str(best_negative["origin_nb_flight"]) + " flights outbound. ")


# Question 8: By continent, what is the average active flight speed ? (flight localization by airport of origin)
