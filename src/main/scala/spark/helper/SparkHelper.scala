package scala.spark.helper

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
 * Trait to define spark session
 *
 * @author Sulfikkar Basheer Shylaja
 * @version 1.0
 *
 */
trait SparkConfiguration {

  lazy val spark =
    SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

  /**
   *  Metod to stop spark session
   */
  def stopSpark(): Unit = spark.stop()
}