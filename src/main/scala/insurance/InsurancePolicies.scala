package insurance

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object InsurancePolicies {

  def policiesSoldByCounty(sc: SparkContext) {

    val logFile = "src/main/resources/InsuranceSample/Insurance_Sample.csv"
    val inputData = sc.textFile(logFile).cache()
    val withoutHeader = dropHeader(inputData)

    //List the policies sold by county in desc order
    val policiesSoldByCounty = withoutHeader.flatMap(line ⇒ line.split("/n") )
      .map (line => (line.split(","))(2))
      .countByValue().toSeq.sortWith( _._2 > _._2)//.foreach(println)



    Path("output/PoliciesSoldBycounty").deleteRecursively()
    sc.parallelize(policiesSoldByCounty).coalesce(1).saveAsTextFile("output/PoliciesSoldBycounty")


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
