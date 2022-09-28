from pyspark.sql.functions import *

def update_countries_continents(countries_continents_df, spark):
    countries_continents_df = countries_continents_df.withColumn("country",
                                when(col("country") == "US", "United States")
                                .when(col("country") == "Korea, South", "South Korea")
                                .when(col("country") == "Russian Federation", "Russia")
                                .when(col("country") == "CZ", "Czechia")
                                .when(col("country") == "Bosnia and Herzegovina", "Bosnia And Herzegovina")
                                .when(col("country") == "Congo, Democratic Republic of", "Democratic Republic Of The Congo")
                                .otherwise(col("country")))
                                
    vals = [("Asia", "Hong Kong"), ("Asia", "Taiwan"), ("North America", "Puerto Rico")]
    newRows = spark.createDataFrame(vals, ["continent", "country"])

    return countries_continents_df.union(newRows)