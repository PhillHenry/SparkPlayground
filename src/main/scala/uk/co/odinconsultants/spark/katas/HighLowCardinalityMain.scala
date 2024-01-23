package uk.co.odinconsultants.spark.katas
import org.apache.spark.sql.Dataset
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark
import uk.co.odinconsultants.spark.katas.SparkUtils.numOfPartitionsIn

object HighLowCardinalityMain {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val numLeft: Long  = 100000L
    val numRight: Long = 1000L
    val nPartitions    = 256
    println(s"Creating data frame with $numLeft rows")
    val left           = spark.range(0, numLeft).as[Long] // .repartition(nPartitions)
    val fileName       = "/tmp/HighLowCardinalityMain_parquet"
    if (!new java.io.File(fileName).exists()) highCardinalityToDisk(left, numRight.toInt, fileName)
    println(s"Reading $fileName")
    def right          = spark.read.parquet(fileName)     // .repartition(nPartitions)
    println(
      s"Number of partitions: left = ${numOfPartitionsIn(left)}, right = ${numOfPartitionsIn(right)}"
    )
    val result         = left.join(right, $"id" === $"_2")
    val count          = result.count()
    println(s"Number of partitions: result = ${numOfPartitionsIn(result)}")
    println(s"count = $count")
  }

  private def highCardinalityToDisk(
      left:     Dataset[Long],
      numRight: Int,
      fileName: String,
  ): Unit = {
    println(s"Writing $fileName")
    left
      .mapPartitions { rows: Iterator[Long] =>
        rows.flatMap(j => Iterator.range(0, numRight).map(i => (i, j)))
      }
      .write
      .parquet(fileName)
  }
}
