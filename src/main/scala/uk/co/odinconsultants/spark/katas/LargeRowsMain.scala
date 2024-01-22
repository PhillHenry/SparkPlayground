package uk.co.odinconsultants.spark.katas
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark

import java.lang
import scala.util.Random

object LargeRowsMain {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val numLeft: Long  = 10000000L
    val numRight: Long = 1000L
    val stringLength   = 10000
    createBigRows(numLeft, stringLength)
  }

  private def createBigRows(numLeft: Long, stringLength: Int): Unit = {
    val left = spark.range(0, numLeft).map { case i: lang.Long =>
      (i, Random.alphanumeric.filter(_ => true).take(stringLength).mkString(""))
    }
  }
}
