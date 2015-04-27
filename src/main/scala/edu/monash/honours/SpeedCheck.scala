package edu.monash.honours

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

object SpeedCheck
{
  val SPEED_UPPER_LIMIT = 85
  val SPEED_LOWER_LIMIT = 0

  def main(args: Array[String])
  {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SpeedCheck")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val speedStream = ssc.socketTextStream("localhost", 9999)

    speedStream.map(checkSpeed)

    speedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def checkSpeed(speedValue: String): (String, Boolean) = {
    if (speedValue.asInstanceOf[Double] > SPEED_UPPER_LIMIT || speedValue.asInstanceOf[Double] < SPEED_LOWER_LIMIT) {
      (speedValue, false)
    } else {
      (speedValue, true)
    }
  }
}
