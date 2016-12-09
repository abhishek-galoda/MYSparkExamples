package calculatePI

import org.apache.spark.SparkContext

/**
  * Created by ABHISHEKG on 18/09/2016.
  */
object PI {

  def pi(sc: SparkContext) = {
    val sample=100000
    val count = sc.parallelize(1 to sample).map { i =>
      val x = Math.random()
      val y = Math.random()
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / sample)
  }
}
