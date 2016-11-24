package ParseCSV

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object ParseCSVGroupByColumn {

  def groupByColumn(sc: SparkContext) {

    val logFile = "SPARK_HOME/Insurance_Sample.csv" // Should be some file on your system
    val inputData = sc.textFile(logFile).cache()
    val withoutHeader = dropHeader(inputData)
    val count = withoutHeader.flatMap(line ⇒ line.split("/n") )
      .map (line => (line.split(","))(2))
      .countByValue().toSeq.sortWith( _._2 > _._2).foreach(println)

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
