package uk.co.odinconsultants.spark.katas
import org.apache.spark.sql.SaveMode
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark

object CountMain {

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val initialNumRows: Long = 1000L
    val df                   = spark.range(0, initialNumRows).as[Long]
    val file                 = "/tmp/test_parquet"
    df.write.mode(SaveMode.Overwrite).parquet(file)
    val parquetFile          = spark.read.parquet(file)
    parquetFile.count()
  }

}
