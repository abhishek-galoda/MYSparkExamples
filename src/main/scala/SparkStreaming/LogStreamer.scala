package sparkstreaming

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._


object LogStreamer {

  def logStreamer(sc: SparkContext) {

    val ssc = new StreamingContext(sc, Seconds(20))

    val fileStream = ssc.textFileStream("file:///C:/Karaf/instances/defaultIntegration1/data/log/karaf.log")

    val count=fileStream.flatMap(line => line.split("\n")).filter(x => x.contains("INFO"))
    println("Total number of line having INFO is :"+ count.count())

    val webservices=count.filter(x => x.contains("Inbound Message")).count()
    println("Total number of web services are :"+ webservices)

    webservices.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(200000)

  }
}
