# Flight Data Statistics

### Application to generate Flight Statistics
The application takes CSV files with data about the flights and the passenger details - FlightsData.csv and Passengers.csv
It generates the statistics for
* Number of flights in a month
* 100 most frequent fliers
* The most number of countries a passenger has travelled without being in the UK
* Passengers who have flown together in more than 3 flights

### 1. Number of flights in a month
Spark SQL is used to group the data in flightData.csv by year and month to find the number of flights in a month.

Sample Output

![Screen Shot 2022-08-02 at 11 33 02 pm](https://user-images.githubusercontent.com/26584709/182387785-c6c5f8e4-000b-42d4-abd7-6e2492de1027.png)







### 2. 100 most frequent fliers
Dataframes created from flightData.csv and passengers.csv are joined and grouped on Passenger ID and sorted to get the most frequent fliers

Sample Output

![Screen Shot 2022-08-02 at 11 33 07 pm](https://user-images.githubusercontent.com/26584709/182387748-9fa846eb-1bfd-4a74-827f-a6f73b0b469d.png)



### 3. The most number of countries a passenger has travelled without being in the UK
flightsData.csv data is queried , the starting location of the passenger's route is found and all the destinations are collected in a list.


### 4. Passengers who have flown together in more than 3 flights
Self join on the Dataframe generated fron the flightsData.csv is used to solve this problem. Join is on the flight ID and the date of the flight columns to get the passengers who flew together on the same flight.The data is grouped on the passenger IDs and the aggregated count is checked to get the result.

Sample Output

![Screen Shot 2022-08-02 at 11 33 15 pm](https://user-images.githubusercontent.com/26584709/182387594-b392de5c-d789-4203-a4a3-46de7c4ce6ba.png)

## Developed Using
* Apache Spark v3.2.1
* Scala v2.13.8
* sbt v1.7.1

## Steps to package and run the application
Clone the repository in IntelliJ IDE

git clone ssh url

Run 'FlightData' from the IDE

Creating the Jar -
From the sbt shell , enter the commans 'package' to generate the jar.
The jar will be generated in the /target/scala-2.13 folder

Run the jar file using spark-submit command
spark-submit --class com.quantexa.execution.FlightData --master local 'path to the jar file'

Run the

## Authors
* **Rishina Chittezhuth Valsalan**


### To Do
1. From the output of Problem 3,  the list has to be looped to find the greatest number of locations travelled without or before being in UK.
2. Add testcases for all the scenarios.

### Version
* 1.0.0




