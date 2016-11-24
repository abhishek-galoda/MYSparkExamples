package SparkStreaming

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object LogStreamer {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark_Examples")
    val ssc = new StreamingContext(conf, Seconds(20))

    val fileStream = ssc.textFileStream("file:///C:/Karaf/instances/defaultIntegration1/data/log/karaf.log")

    val count=fileStream.flatMap(line => line.split("\n")).filter(x => x.contains("INFO"))

    count.saveAsTextFiles("SPARK_HOME/logger"+ System.currentTimeMillis())

    ssc.start()
    ssc.awaitTerminationOrTimeout(200000)

  }
}
