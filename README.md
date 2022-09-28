# FlightRadar24

## Author

Kevin Huynh     - kevin.huynh@epita.fr

## Launch code

Install dependencies

```
pip install -r requirements.txt
```

Execute the code
```
python3 src/main.py
```


## Questions 

### Question 1:  What is the company with the most active flights in the world ?

For this question, I simply use the flights API request and count the number of flights groupby the company icao. The company which have the better count is logically the one with the most active flights in the world. 


------
### Question 2: By continent, what are the companies with the most regional active flights (airports of Origin & Destination within the same continent) ?

For this question, I first make the differents joins to find countries of origin and destination airports between flights and airports dataframe.

To find if the 2 airports are located on the same continent, I used an external dataset found here 
https://github.com/dbouquin/IS_608/blob/master/NanosatDB_munging/Countries-Continents.csv which references a country with its continent. 
It was the dataset that had the closest data to those of FlightRadar24's API. But I still had to do some preprocessing on these data. For exemple, United States is written as US in the external dataset or South Korea is written as Korea, South ... So I had to update the data so that it matchs the API. 
And sometimes, some countries (island or states) like Hong Kong, Taiwan ... wasn't referenced so I add it on the dataset. 

Thanks to this external dataset, I joined it to find the origin continent and destination continent so that I can filter data only when the flights belong to the same origin and destination continent.

After that, I just need to groupby data within continent and company to find in each continent the company with the most regional active flights. 

NB: Maybe some countries are not referenced well betweek the external dataset and the country of airports dataset, I made the fix with the data I have worked on. 


-----
### Question 3: World-wide, Which active flight has the longest route ?
For this question, I first joined the flights dataset with the airport dataset to find the localization (longitude, latitude and altitude) of the origin and destination airport.

Then I used the geopy library to calculate the distance in 2d between the 2 airports thanks to latitude and longitude
I used the Pythagore theorem to find the real distance depending on the altitude of each airport. 

Finally, collecting the largest distance between 2 airports => find the active flight with the longest route


-----
### Question 4: By continent, what is the average route distance ? (flight localization by airport of origin)
For this question, I joined dataframes to find the country and the continent of the airport of origin (it is the one who determine the continent) and I also joined to find the coordinate of origin and destination airport. 

I used the same method as question 3 to calculate the distance between 2 airports. 

Finally I groupby datas according to the continent of origin airport and make an average of these distance to find the average route distance of flights by continent. 

-----
### Question 5.1: Which leading airplane manufacturer has the most active flights in the world ?


-----
### Question 5.2: By continent, what is the most frequent airplane model ? (airplane localization by airport of origin)
For this question, like the other questions I joined dataframes to find the country and the continent of the airport of origin (it is the one who determine the continent).

After that, I just needed to group data by continent and aircraft code (that is the model of the airplane) so that I can collect for each continent the most frequent airplane model. 

-----
### Question 6: By company registration country, what are the tops 3 airplanes model flying ?


-----
### Question 7.1: By continent, what airport is the most popular destination ?
For this question, unlike the other questions by continent which depend on airport of origin, I joined dataframes to find the country and continent of the airport of destination.

Then, I just group datas by continents and airports to find for each continent the airport which is the most popular destination.

-----
### Question 7.2: What airport has the greatest inbound/outbound flights difference ? (positive or negative)
For this question, I just used the flights dataframe. On that one, I made two groupby: one depending on origin_airport column and the other depending on destination_airport_column. For the two, I count for each airport, how many flights are inbound and outbound. Thus, I can then made the difference between count(inbound) and count(outbound) to fiund the 2 greatest difference inbound/outbound positively and negatively

-----
### Question 8: By continent, what is the average active flight speed ? (flight localization by airport of origin)
For this question, I joined again dataframes to find the country and the continent of the airport of origin. 

Then I group datas by continent and make an average on the ground speed to find the average active flight speed by continent. 