package sulfi.spark.pressure

import java.io.FileNotFoundException
import scala.util.Failure

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import sulfi.weather.utils.pressure
import sulfi.spark.weather.helper.SparkConfiguration
import sulfi.weather.schema.pressure.SchemaGeneral
import sulfi.weather.schema.pressure.Schema1938
import sulfi.weather.schema.pressure.Schema1756
import sulfi.weather.schema.pressure.Schema1859

/**
 * Spark program that transforms pressure data and stores it in hive table.
 *
 * @author Sulfikkar Basheer Shylaja
 * @version 1.0
 */
    object pressurespark extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)

  /**
   * Entry point to the application
   *
   * @param args
   */
  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Pressure transformation and analysis")
    log.info("Pressure weather data transformation and analysis started")
    pressureAnalysis(spark)
    stopSpark()
  }

  /**
   * Pressure weather data transformation and analysis
   *
   * @param sparkSession
   */
  def pressureAnalysis(sparkSession: SparkSession): Unit = {

    // Reading property file
    val pressUtils = new pressureUtils()
    val properties = pressUtils.readPropertyFile()

    try {

      // ================================================================
      //               Manual Station Pressure Data
      //=================================================================
      val manualPressureDataRDD = sparkSession
        .sparkContext
        .textFile(properties.getProperty("pressure.manual.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => GeneralSchema(column(0), column(1), column(2), column(3), column(4), column(5)))

      val manualPressureDataTempDF = sparkSession.createDataFrame(manualPressureDataRDD)
      val manualPressureSchemaDF = manualPressureDataTempDF
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val manualPressureDataDF = manualPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //               Automatic Station Pressure Data
      //=================================================================

      val automaticPressureRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.automatic.input.dir"))
        .map(item => item.split("\\s+"))
        .map(column => GeneralSchema(column(0), column(1), column(2), column(3), column(4), column(5)))
      val automaticPressureTempDF = sparkSession.createDataFrame(automaticPressureRDD)
      val automaticPressureSchemaDF = automaticPressureTempDF
        .withColumn("station", lit("Automatic"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
      val automaticPressureDataDF = automaticPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //               Cleaned  Pressure Data (1938_hpa)
      //=================================================================

      val pressureData1938RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1938.input.dir"))
        .map(item => item.split("\\s+"))
        .map(x => Schema1938(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val pressureData1938TempDF = sparkSession.createDataFrame(pressureData1938RDD)
      val pressureData1938SchemaDF = pressureData1938TempDF
        .drop("space")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureData1938DF = pressureData1938SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //               Cleaned  Pressure Data (1862_mmhg)
      //=================================================================

      val pressureData1862RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1862.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1938(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val pressureData1862TempDF = sparkSession.createDataFrame(pressureData1862RDD)
      val pressureData1862SchemaDF = pressureData1862TempDF
        .drop("space")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("mmhg"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureData1862DF = pressureData1862SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //                 Pressure Data (1756)
      //=================================================================

      val pressureData1756RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("pressure.1756.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1756(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      val pressureData1756TempDF = sparkSession.createDataFrame(pressureData1756RDD)
      val pressureData1756SchemaDF = pressureData1756TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureData1756DF = pressureData1756SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //                Pressure Data (1859)
      //=================================================================
      val pressureData1859RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("presssure.1859.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Schema1859(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))
      val pressureData1859TempDF = sparkSession.createDataFrame(pressureData1859RDD)
      val pressureData1859SchemaDF = pressureData1859TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))

      val pressureData1859DF = pressureData1859SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // ================================================================
      //                Pressure Data (1961)
      //=================================================================
      val pressureData1961RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("presssure.1961.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => GeneralSchema(x(0), x(1), x(2), x(3), x(4), x(5)))
      val pressureData1961TempDF = sparkSession.createDataFrame(pressureData1961RDD)
      val pressureData1961SchemaDF = pressureData1961TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureData1961DF = pressureData1961SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      // Final transformed pressure data
      val pressureDF = manualPressureDataDF
        .union(automaticPressureDataDF)
        .union(pressureData1938DF)
        .union(pressureData1862DF)
        .union(pressureData1756DF)
        .union(pressureData1859DF)
        .union(pressureData1961DF)

      // ================================================================
      //             Save pressure data to hive table
      //=================================================================
      import spark.sql
      sql("""CREATE TABLE PressureData(
            year String, 
            month String, 
            day String, 
            pressure_morning String, 
            pressure_noon String, 
            pressure_evening String, 
            station String, 
            pressure_unit String, 
            barometer_temperature_observations_1 String, 
            barometer_temperature_observations_2 String, 
            barometer_temperature_observations_3 String, 
            thermometer_observations_1 String, 
            thermometer_observations_2 String, 
            thermometer_observations_3 String, 
            air_pressure_reduced_to_0_degC_1 String, 
            air_pressure_reduced_to_0_degC_2 String, 
            air_pressure_reduced_to_0_degC_3 String) 
          STORED AS PARQUET""")
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("PressureData")

      // ================================================================
      //             Data Reconcilation
      //=================================================================
      val totalInputCount = manualPressureDataRDD.count() +
        automaticPressureRDD.count() +
        pressureData1938RDD.count() +
        pressureData1862RDD.count() +
        pressureData1756RDD.count() +
        pressureData1859RDD.count() +
        pressureData1961RDD.count()
      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + pressureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) as count FROM PressureData").show(false))

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