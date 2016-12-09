package weather

/**
  * Created by abhishekg on 08/09/2016.
  */
import org.apache.spark.SparkContext

import scala.reflect.io.Path


object WeatherData {

  def minWeatherReportedByStation(sc: SparkContext): Unit = {

    val file = "src/main/resources/DataSet/1800.csv"
    val inputData = sc.textFile(file).cache()

    //Delete the exisiting file if any
    Path("output/MinWeatherByStation").deleteRecursively()

    //Get station, entry type and Temp
    val rdd=inputData.map(getStationEntryAndTemp)

    val onlyMinTemp=rdd.filter(x => x._2 == "TMIN")

    val stationTemps=onlyMinTemp.map(x => (x._1,x._3.toFloat))

    val results=stationTemps.reduceByKey((x,y) => if(x<y) x else y).collect()

    sc.parallelize(results).coalesce(1).saveAsTextFile("output/MinWeatherByStation")

    for (result <- results){
      val station = result._1
      val temp = result._2

      println(s"$station has minimum temp: $temp")
    }
  }

  def getStationEntryAndTemp(line:String) ={
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType= fields(2)
    val temp = fields(3).toFloat * .01 *(9/5)+ 32
    (stationId,entryType,temp)

  }
}
