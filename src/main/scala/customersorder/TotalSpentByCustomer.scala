package customersorder

import org.apache.spark.SparkContext

import scala.reflect.io.Path

/**
  * Created by abhishekg on 09/10/2016.
  */
object TotalSpentByCustomer {

  def totalAmountSpentByCustomer(sc: SparkContext): Unit = {
    val file = "src/main/resources/DataSet/customer-orders.csv"
    val inputData = sc.textFile(file).cache()

    //Delete the exisiting file if any
    Path("output/CustomerOrder").deleteRecursively()

    //Get ID and Amount
    val rdd = inputData.map(getCustomerIDandAmount)

    val results =rdd.reduceByKey(_+_)

    val sortedResults=results.map(x => (x._2,x._1)).sortByKey()

    sortedResults.coalesce(1).saveAsTextFile("output/CustomerOrder")
  }

  def getCustomerIDandAmount(line: String) = {
    val fields = line.split(",")
    val id = fields(0).toInt
    val amount = fields(2).toFloat

    (id, amount)

  }

}