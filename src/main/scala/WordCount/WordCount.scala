package wordcount

import org.apache.spark.SparkContext

import scala.reflect.io.Path

object WordCount {

  def wordCount(sc: SparkContext) {
    val logFile = "src/main/resources/README.md" // file is under src/main/resources

    val input = sc.textFile(logFile)
    val count = input.flatMap(_.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    Path("output/wordcount").deleteRecursively()
    count.saveAsTextFile("output/wordcount")

  }
}
