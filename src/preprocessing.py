from pyspark.sql.functions import *

def update_countries_continents(countries_continents_df, spark):
    countries_continents_df = countries_continents_df.withColumn("country",
                                when(col("country") == "US", "United States")
                                .when(col("country") == "Korea, South", "South Korea")
                                .when(col("country") == "Russian Federation", "Russia")
                                .when(col("country") == "CZ", "Czechia")
                                .when(col("country") == "Bosnia and Herzegovina", "Bosnia And Herzegovina")
                                .when(col("country") == "Congo, Democratic Republic of", "Democratic Republic Of The Congo")
                                .when(col("country") == "Antigua and Barbuda", "Antigua And Barbuda")
                                .when(col("country") == "Trinidad and Tobago", "Trinidad And Tobago")
                                .when(col("country") == "Ivory Coast", "Cote D'ivoire (Ivory Coast)")
                                .when(col("country") == "Burkina", "Burkina Faso")
                                .otherwise(col("country")))
                                
    vals = [("Asia", "Hong Kong"), ("Asia", "Taiwan"), ("North America", "Puerto Rico"), ("Oceania", "Guam"), \
            ("North America", "Martinique"), ("Oceania", "New Caledonia"), ("South America", "Curacao"), ("Oceania", "Northern Mariana Islands"), \
            ("Africa", "Reunion"), ("Africa", "Mayotte"), ("North America", "Guadeloupe"), ("Oceania", "French Polynesia"), \
            ("South America", "Aruba"), ("North America", "Turks And Caicos Islands"), ("South America", "French Guiana"), \
            ("North America", "Saint Vincent And The Grenadines"), ("North America", "Bermuda"), ("Asia", "Macao"), ("North America", "Greenland")]
    newRows = spark.createDataFrame(vals, ["continent", "country"])

    return countries_continents_df.union(newRows)