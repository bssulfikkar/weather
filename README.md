## Weather Loading ##

Loading the Stockholm temperature and 
pressure data to hive tables after doing required cleaning using spark and scala.

## Using ##

1. Scala
2. Spark 
3. Hadoop
3. Hive
4. Maven


## Prerequisite ##

1. Download the data files from:


- Temperature Data: 

https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php


- Pressure Data: 

https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php



2. Move the files to HDFS path and configure the path in "temperatureconfig.properties" file


| Key | Value |
| ------ | ------ |
| temperature.manual.input.dir | hdfs location where manual station temperature reading data resides |
| temperature.automatic.input.dir | hdfs location where automatic station temperature reading data resides |
| temperature.space.input.dir | hdfs location where space preceding temperature reading data resides  |
| temperature.actual.input.dir | hdfs location where temperature reading data with correct schema resides  |

3. Move the files to HDFS path and configure the path in "pressureconfig.properties" file

| Key | Value |
| ------ | ------ |
| pressure.automatic.input.dir | hdfs location where manual station pressure data resides |
| pressure.manual.input.dir | hdfs location where automatic station pressure data resides |
| pressure.1938.input.dir | hdfs location where pressure data from 1938 resides |
| pressure.1862.input.dir | hdfs location where pressure data from 1862 resides  |
| pressure.1756.input.dir | hdfs location where pressure data from 1756 resides  |
| pressure.1859.input.dir | hdfs location where pressure data from 1859 resides |
| pressure.1961.input.dir |hdfs location where pressure data from 1961 resides  |



4. Move the files to HDFS path and map to  file key in below manner

"Temperature"

- "stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"    to value mentioned for the key "temperature.manual.input.dir" in property file
- "stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"    to  value mentioned for the key "temperature.automatic.input.dir" in property file
- "stockholm_daily_temp_obs_1756_1858_t1t2t3.txt"   to  value mentioned for the key "temperature.space.input.dir" in property file
- "stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt"  to  value mentioned for the key "temperature.actual.input.dir" in property file
- "stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt"  to  value mentioned for the key "temperature.actual.input.dir" in property file

"Pressure"

- "stockholm_barometer_2013_2017.txt"    to value mentioned for the key "pressure.manual.input.dir" in property file
- "stockholmA_barometer_2013_2017.txt"    to value mentioned for the key "pressure.automatic.input.dir" in property file
- "stockholm_barometer_1938_1960.txt"    to value mentioned for the key "pressure.1938.input.dir" in property file
- "stockholm_barometer_1862_1937.txt"   to value mentioned for the key "pressure.1862.input.dir" in property file
- "stockholm_barometer_1756_1858.txt"   to value mentioned for the key "pressure.1756.input.dir" in property file
- "stockholm_barometer_1859_1861.txt"   to value mentioned for the key "pressure.1859.input.dir" in property file
- "stockholm_barometer_1961_2012.txt"  to value mentioned for the key "pressure.1961.input.dir" in property file

## Code ##

// Create dummy data and load it into a DataFrame
case class rowschema(id:Int, record:String)
val df = sqlContext.createDataFrame(Seq(rowschema(1,"record1"), rowschema(2,"record2"), rowschema(3,"record3")))
df.registerTempTable("tempTable")

// Create new Hive Table and load tempTable
sqlContext.sql("create table newHiveTable as select * from tempTable")

## End of Code ##

## Installation ##

1. Build the jar using maven

"mvn clean install"

2. Upload the jar file built to the cluster driver node location

3. Execute: 

- Temperature Clean Up

"spark-submit --class com.sulfi.spark.weather.Pressure --master yarn <path to weather-1.0.jar>"

- Pressure Clean Up

"spark-submit --class com.sulfi.spark.weather.Temperature --master yarn <path to weather-1.0.jar>"