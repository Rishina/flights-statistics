package com.quantexa.execution

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**  Read the input files flightData.csv , passenger.csv and create local temporary views.
  * Use the views to generate the statistics
  */
object FlightsData {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("FlightsData")
    .getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    val flightsDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(
        System.getProperty("user.dir") + "/src/main/data/flightData.csv"
      )
    flightsDF.createOrReplaceTempView("flights")

    val passengersDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(
        System.getProperty("user.dir") + "/src/main/data/passengers.csv"
      )
    passengersDF.createOrReplaceTempView("passengers")

    monthlyFlights(flightsDF)
    mostFrequentFliers(flightsDF, passengersDF)
    greatestNoOfFlights
    passengersFlownTogether
    flownTogether(2, "2017-01-01", "2017-01-25")

    spark.stop()
  }

  /** Find the number of flights in each month
    */
  def monthlyFlights(flightsDF: sql.DataFrame): DataFrame = {
    val monthlyFlights = spark
      .sql(
        "SELECT  year(date) , month(date) ,count(distinct(flightid)) as flights from flights group by year(date), month(date)"
      )
      .sort("month(date)")
      .select("month(date)", "flights")
      .withColumnRenamed("month(date)", "Month")
      .withColumnRenamed("flights", "No. of Flights")

    monthlyFlights.write
      .format("csv")
      .option("header", "true")
      .save(
        System.getProperty("user.dir") + "/src/main/output/MonthlyFlights"
      )
    monthlyFlights
  }

  /** Find the 100 msst frequent fliers
    */
  def mostFrequentFliers(
      flightsDF: sql.DataFrame,
      passengersDF: sql.DataFrame
  ): DataFrame = {
    import spark.sqlContext.implicits._
    val frequentFliers = flightsDF
      .as("f1")
      .groupBy("passengerId")
      .count()
      .sort(col("count").desc)
    frequentFliers
      .join(
        passengersDF,
        Seq("passengerId"),
        "inner"
      )
      .sort(col("count").desc)
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("count", "No. of Flights")
      .withColumnRenamed("firstName", "First name")
      .withColumnRenamed("lastName", "Last name")
      .toDF()
      .show(100)

    frequentFliers.write
      .format("csv")
      .option("header", "true")
      .save(
        System.getProperty("user.dir") + "/src/main/output/FrequentFliers"
      )

    frequentFliers
  }

  /** Passengers those have flown together in more than 3 flights
    */
  def passengersFlownTogether() {
    val passengerDF = spark
      .sql("SELECT passengerId , flightId, date from flights")
      .toDF("passengerId", "flightId", "date")

    import spark.sqlContext.implicits._
    val flownTogetherDF = passengerDF
      .as("p1")
      .join(
        passengerDF as ("p2"),
        $"p1.flightId" === $"p2.flightId" &&
          $"p1.date" === $"p2.date" &&
          $"p1.passengerId" =!= $"p2.passengerID",
        "inner"
      )
      .groupBy($"p1.passengerId", $"p2.passengerID")
      .agg(count("p1.flightId"))
      .where(count("p1.flightId") > 3)
      .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together")
      .orderBy(col("Number of flights together").desc)

    flownTogetherDF.write
      .format("csv")
      .option("header", "true")
      .save(
        System.getProperty("user.dir") + "/src/main/output/FlownTogether"
      )
  }

  def flownTogether(atLeastNTimes: Int, from: String, to: String) = {
    val passengerDF = spark
      .sql("SELECT passengerId , flightId, date from flights")
      .toDF("passengerId", "flightId", "date")

    import spark.sqlContext.implicits._
    val flownTogetherFilteredDF = passengerDF
      .as("p1")
      .join(
        passengerDF as ("p2"),
        $"p1.flightId" === $"p2.flightId" &&
          $"p1.date" === $"p2.date" &&
          $"p1.passengerId" =!= $"p2.passengerID",
        "inner"
      )
      .filter($"p1.date".between(from, to))
      .groupBy($"p1.passengerId", $"p2.passengerID")
      .agg(count("p1.flightId"))
      .where(count("p1.flightId") > atLeastNTimes)
      .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together")
      .orderBy(col("Number of flights together").desc)

    flownTogetherFilteredDF.write
      .format("csv")
      .option("header", "true")
      .save(
        System.getProperty(
          "user.dir"
        ) + "/src/main/output/FlownTogetherFiltered"
      )
  }

  /** Find the greatest number of flights for a passenger without connecting to UK
    */

  def greatestNoOfFlights() {
    val flightRouteDF = spark
      .sql(
        "SELECT passengerId , flightId, from, to, date from flights order by date"
      )
      .toDF("passengerId", "flightId", "from", "to", "date")
      .groupBy("passengerId")
      .agg(concat(first("from")), collect_list("to"))
      .toDF("passengerId", "start", "route")
      .show()

  }
}
