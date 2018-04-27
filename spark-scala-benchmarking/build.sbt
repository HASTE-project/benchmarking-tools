name := "spark-scala-benchmarking"

version := "0.1"

// Spark is built against Scala 2.11 - this is latest version of 2.11
scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.0"


// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"