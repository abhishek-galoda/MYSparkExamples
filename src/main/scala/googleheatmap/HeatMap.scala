package googleheatmap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object HeatMap {

  def getLatitudeAndLongitude(sc: SparkContext) {

    val logFile = "src/main/resources/InsuranceSample/Insurance_Sample.csv" // Should be some file on your system
    val inputData = sc.textFile(logFile).cache()
    val withoutHeader = dropHeader(inputData)
    val validLines = withoutHeader.flatMap(line ⇒ line.split("/n")).filter(line => line.split(",").length > 14)

    Path("output/LatLong").deleteRecursively()

    validLines.map {
      x => {
        val split = x.split(",")
        (split(13),split(14))
      }
    }.saveAsTextFile("output/LatLong")

  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((index, lines) => {
      if (index == 0) {
        lines.drop(1)
      }
      lines
    })
  }


}