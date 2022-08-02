package com.quantexa.execution

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Date

class FlightsDataSpec extends AnyFlatSpec with DataFrameComparer {

  case class Flights(
      passengerId: Int,
      flightId: Int,
      from: String,
      to: String,
      date: String
  )
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("FlightsData")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    it should ("show monthly flights") in {

      val flightsData = Seq(
        Flights(501, 1, "ca", "ir", "2017-01-10"),
        Flights(502, 1, "ca", "ir", "2017-01-12"),
        Flights(503, 2, "at", "be", "2017-02-10"),
        Flights(504, 3, "uk", "mb", "2017-05-10"),
        Flights(501, 4, "ir", "uk", "2017-05-10")
      )

      import spark.implicits._
      val flightsDF = flightsData.toDF()
      val result = FlightsData.monthlyFlights(flightsDF)
      val expectedDataDF = Seq(
        (1, 2),
        (2, 1),
        (5, 2)
      ).toDF()

      assertSmallDataFrameEquality(result, expectedDataDF)
    }
    spark.stop()

  }
}
