package sulfi.spark.temperature

import java.io.FileNotFoundException
import scala.util.Failure
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import sulfi.spark.weather.helper.SparkConfiguration
import sulfi.weather.schema.temperatureschema.ActualSchema
import sulfi.weather.schema.temperatureschema.UncleanSchema
import sulfi.weather.schema.temperatureschema.AutomaticSchema
import sulfi.weather.schema.temperatureschema.ManualSchema
import sulfi.weather.utils.temperature

/**
 * Spark program that transforms temperature data and stores it in hive table.
 *
 * @author Sulfikkar Basheer Shylaja
 * @version 1.0
 */
object temperaturespark extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)

  /**
   * Entry point to the application
   *
   * @param args
   */
  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Temperature Data Cleaning")
    log.info("Temperature weather data transformation and analysis started")
    temperatureAnalysis(spark)
    stopSpark()
  }

  /**
   * Temperature Data Cleaning
   *
   * @param sparkSession
   */
  def temperatureAnalysis(sparkSession: SparkSession): Unit = {

    // Reading property file
    val tempUtils = new temperatureUtils()
    val properties = tempUtils.readPropertyFile()

    try {
      // ================================================================
      //               Manual Station Temperature Data
      //=================================================================
      // Read input data
      val manualStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.manual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column =>
          ManualSchema(
            column(0), column(1), column(2), column(3), column(4), column(5), column(6), column(7), column(8)))
      val manualStationTempDF = sparkSession.createDataFrame(manualStationTempRDD)
      // Add station column
      val manualStationDF = manualStationTempDF.withColumn("station", lit("manual"))

      // ================================================================
      //               Automatic Station Temperature Data
      //=================================================================
      // Read automatic reading temperature data
      val automaticStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.automatic.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => AutomaticSchema(
          column(0), column(1), column(2), column(3), column(4), column(5), column(6), column(7), column(8)))
      val automaticStationTempDF = sparkSession.createDataFrame(automaticStationTempRDD)
      // Add station column
      val automaticStationDF = manualStationTempDF.withColumn("station", lit("automatic"))

      // ================================================================
      //               Space contained temperature data
      //=================================================================
      val spaceTempDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.space.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => UncleanSchema(column(0), column(1), column(2), column(3), column(4), column(5), column(6)))
      val uncleanTempDataDF = sparkSession.createDataFrame(uncleanTempDataRDD)

      // Add necessary columns to unify all the input data
      val uncleanTempCleansedDF = uncleanTempDataDF
        .drop("extra")
        .withColumn("tmin", lit("NaN"))
        .withColumn("tmax", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

      // ================================================================
      //              Read non changed schema temperature data
      //=================================================================
      val temperatureDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("temperature.actual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => ActualSchema(column(0), column(1), column(2), column(3), column(4), column(5)))
      val temperatureDataTempDF = sparkSession.createDataFrame(temperatureDataRDD)
      val temperatureDataDF = temperatureDataTempDF
        .withColumn("tmin", lit("NaN"))
        .withColumn("tmax", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))

      // Joining all the input data to make as one data frame
      val temperatureDF = manualStationDF
        .union(automaticStationDF)
        .union(uncleanTempCleansedDF)
        .union(temperatureDataDF)

      // ================================================================
      //             Save temperature data to hive table
      //=================================================================
      import spark.sql
      // Create hive table query
      sql("""CREATE TABLE TemperatureData(
        year String, 
        month String, 
        day String, 
        morning String, 
        noon String, 
        evening String, 
        tmin String, 
        tmax String, 
        estimated_diurnal_mean String, 
        station String)
      STORED AS PARQUET""")
      // Write to hive table created above.
      temperatureDF.write.mode(SaveMode.Overwrite).saveAsTable("TemperatureData")

      // ================================================================
      //             Data Reconcilation
      //=================================================================
      val totalInputCount = manualStationTempRDD.count() +
        automaticStationTempRDD.count() +
        spaceTempDataRDD.count() +
        temperatureDataRDD.count()
      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + temperatureDF.count())
      // sql("SELECT * FROM Temperature_Data").show(100, false)
      log.info("Hive data count " + sql("SELECT count(*) FROM TemperatureData").show(false))
    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }
  }
}