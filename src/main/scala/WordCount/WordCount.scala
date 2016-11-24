package WordCount

import org.apache.spark.SparkContext

import scala.reflect.io.Path

object WordCount {

  def wordCount(sc: SparkContext) {
    val logFile = "SPARK_HOME/README.md" // Should be some file on your system

    val input = sc.textFile(logFile)
    val count = input.flatMap(_.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    Path("wordcount").deleteRecursively()
    count.saveAsTextFile("wordcount")

  }
}
