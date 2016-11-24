name := "Spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2"

libraryDependencies +="org.apache.spark" %% "spark-streaming" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2"