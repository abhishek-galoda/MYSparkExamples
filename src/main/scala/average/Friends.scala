package average

import org.apache.spark.SparkContext

import scala.reflect.io.Path


object Friends {

  def friendsByAge(sc: SparkContext): Unit = {

    val file = "src/main/resources/DataSet/fakeFriends.csv"
    val inputData = sc.textFile(file).cache()
    val rdd=inputData.map(getAgeAndNumberOfFriends)

    Path("output/AverageFriendsByAge").deleteRecursively()

    //Get Total By age
    val totalByAge=rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2))

    val averageByAge = totalByAge.mapValues(x => x._1/x._2)

    val sortedAverageByAge=averageByAge.collect().sorted

    sc.parallelize(sortedAverageByAge).coalesce(1).saveAsTextFile("output/AverageFriendsByAge")
  }

  def getAgeAndNumberOfFriends(line:String) ={
        ((line.split(",")(2)).toInt,(line.split(",")(3))toInt)
  }
}
