package uk.co.odinconsultants.spark.katas
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark

object BigDataSmallMemoryMain {

  /**
   * Run this with -Xmx512m to demonstrate that 1.4gb is indeed written to disk.
   * On my machine:
   *
   * $ tree /tmp/results_parquet
  /tmp/results_parquet
  ├── part-00000-edbc76a6-6c03-4f79-9a6d-b650219d261e-c000.snappy.parquet
  ├── part-00001-edbc76a6-6c03-4f79-9a6d-b650219d261e-c000.snappy.parquet
  └── _SUCCESS
   * each parquet file about 689mb
   */
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val initialNumRows: Long = 10000000L
    val df            = spark.range(0, initialNumRows).as[Long]
    val multiplier    = 1000
    val fileName      = "/tmp/results_parquet"
    df.mapPartitions { rows: Iterator[Long] =>
      rows.flatMap(_ => Iterator.range(0, multiplier))
    }.write.mode("overwrite").parquet(fileName)
    val actual        = spark.read.parquet(fileName)
    val count         = actual.count()
    val expectedRows  = multiplier * initialNumRows
    println(s"Actual numer of rows = $count, expected = $expectedRows")
    // on my machine:
    // Number of partitions: Original = 2, persisted = 11
    println(s"Number of partitions: Original = ${df.rdd.partitions.length}, persisted = ${actual.rdd.partitions.length}")
    assert(count == expectedRows)
  }

}
