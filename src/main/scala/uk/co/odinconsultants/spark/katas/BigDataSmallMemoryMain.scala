package uk.co.odinconsultants.spark.katas
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark

object BigDataSmallMemoryMain {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val initialNumRows: Long = 10000000L
    val df            = spark.range(0, initialNumRows).as[Long]
    val multiplier    = 1000
    val fileName      = "/tmp/results_parquet"
    df.mapPartitions { rows: Iterator[Long] =>
      rows.flatMap(_ => Iterator.range(0, multiplier))
    }.write.mode("overwrite").parquet(fileName)
    val actual = spark.read.parquet(fileName)
    val count = actual.count()
    val expectedRows = multiplier * initialNumRows
    println(s"Actual numer of rows = $count, expected = $expectedRows")
    assert(count == expectedRows)
  }

}
