import calculatePI.PI
import googleheatmap.HeatMap
import parseCSV.ParseCSV
import SparkStreaming.LogStreamer
import wordcount.WordCount
import org.apache.spark.{SparkContext, _}

object SparkMain {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark_Examples")
    val sc = new SparkContext(conf)

    //count the number of words in a file
    WordCount.wordCount(sc)

    //Calculate the value of  PI
    PI.pi(sc)

    //Parse CSV  and list the insurance policies saved by count in desc order
    ParseCSV.policiesSoldByCounty(sc)

    //Parse CSV and get the Latitude and Longitudes
    HeatMap.getLatitudeAndLongitude(sc)
  }

}
