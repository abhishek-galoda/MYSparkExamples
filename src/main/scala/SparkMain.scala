import average.Friends
import calculatePI.PI
import customersorder.TotalSpentByCustomer
import googleheatmap.HeatMap
import insurance.InsurancePolicies
import org.apache.spark.{SparkContext, _}
import weather.WeatherData
import wordcount.WordCount

/**To Run these examples use **/
//spark-submit --class SparkMain --master local[*] target\scala-2.11\spark_2.11-1.0.jar

object SparkMain {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark_Examples")
    val sc = new SparkContext(conf)

    //count the number of words in a file
    WordCount.wordCount(sc)

    //Calculate the value of  PI
    PI.pi(sc)

    //Parse CSV  and list the insurance policies saved by count in desc order
    InsurancePolicies.policiesSoldByCounty(sc)

    //Parse CSV and get the Latitude and Longitudes
    HeatMap.getLatitudeAndLongitude(sc)

    //Get average number of Friends by age
    Friends.friendsByAge(sc)

    //Get Minimum Data per weather station
    WeatherData.minWeatherReportedByStation(sc)

    //get Total amount spent by a customer
    TotalSpentByCustomer.totalAmountSpentByCustomer(sc)
  }

}
