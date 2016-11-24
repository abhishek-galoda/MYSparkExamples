import CalculatePI.PI
import ParseCSV.ParseCSVGroupByColumn
import SparkStreaming.LogStreamer
import WordCount.WordCount
import org.apache.spark.{SparkContext, _}

object SparkMain {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark_Examples")
    val sc = new SparkContext(conf)

    WordCount.wordCount(sc)
    PI.pi(sc)
    ParseCSVGroupByColumn.groupByColumn(sc)

  }






}
