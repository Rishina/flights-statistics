version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test"

libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"

lazy val root = (project in file("."))
  .settings(
    name := "Flights"
  )
