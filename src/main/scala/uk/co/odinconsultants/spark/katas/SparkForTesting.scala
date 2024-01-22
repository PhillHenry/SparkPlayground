package uk.co.odinconsultants.spark.katas

import org.apache.spark.sql.internal.SQLConf.DEFAULT_CATALOG
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, SPARK_SESSION_EXTENSIONS, WAREHOUSE_PATH}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Files

object SparkForTesting {
  val master   : String    = "local[2]"
  val tmpDir   : String    = Files.createTempDirectory("SparkForTesting").toString
  val sparkConf: SparkConf = {
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    new SparkConf()
      .setMaster(master)
      .setAppName("Tests")
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set(WAREHOUSE_PATH.key, tmpDir)
      .setSparkHome(tmpDir)
  }
  sparkConf.set("spark.driver.allowMultipleContexts", "true")
  val sc: SparkContext       = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession    = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext = spark.sqlContext

}
