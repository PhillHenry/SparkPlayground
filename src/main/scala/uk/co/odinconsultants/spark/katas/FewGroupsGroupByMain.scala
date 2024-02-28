package uk.co.odinconsultants.spark.katas
import uk.co.odinconsultants.spark.katas.SparkForTesting.spark
import org.apache.spark.sql.functions.{lit, collect_set, col}

object FewGroupsGroupByMain {
  /**
   * Run with -Xmx512mb and watch it blow up
   */
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val initialNumRows: Long = 10000000L
    val group_id             = "group_id"
    val df                   = spark.range(0, initialNumRows).as[Long].toDF().withColumn(group_id, lit(1))
    df.show()
    val grouped = df.groupBy(group_id).agg(collect_set(col("id")))
    grouped.show()
  }
}
