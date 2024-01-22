package uk.co.odinconsultants.spark.katas
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark
import uk.co.odinconsultants.spark.katas.SparkUtils.numOfPartitionsIn

object HighLowCardinalityMain {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val numLeft: Long  = 10000000L
    val numRight: Long = 1000L
    val nPartitions    = 256
    val left           = spark.range(0, numLeft).as[Long].repartition(nPartitions)
    def right          = left
      .mapPartitions { rows: Iterator[Long] =>
        rows.flatMap(j => Iterator.range(0, numRight).map(i => (i, j)))
      }
      .repartition(nPartitions)
    println(
      s"Number of partitions: left = ${numOfPartitionsIn(left)}, right = ${numOfPartitionsIn(right)}"
    )
    val result         = left.join(right, $"id" === $"_2")
    val count          = result.count()
    println(s"Number of partitions: result = ${numOfPartitionsIn(result)}")
    println(s"count = $count")
  }

}
