package uk.co.odinconsultants.spark.katas
import org.apache.spark.sql.Dataset

object SparkUtils {

  def numOfPartitionsIn(df: Dataset[_]): Int = df.rdd.partitions.length

}
