## Loading Weather data into Hive table using scala and spark  using scala and spark ##


#Data is available in the below urls#

#Temperature Data:#
https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php


#Pressure Data:#
https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php

#temperatureconfig.properties and pressureconfig.properties files are created to keep the mapping of input files#

#Temperature Input Paths are as below#

temperature.manual.input.dir

temperature.automatic.input.dir

temperature.space.input.dir

temperature.actual.input.dir

##Pressure Input Paths are as below##

pressure.automatic.input.dir

pressure.manual.input.dir

pressure.1938.input.dir

pressure.1862.input.dir

pressure.1756.input.dir

pressure.1859.input.dir

pressure.1961.input.dir

Temperature property file

temperature.manual.input.dir will store stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

temperature.automatic.input.dir will store stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

temperature.space.input.dir will store stockholm_daily_temp_obs_1756_1858_t1t2t3.txt

temperature.actual.input.dir will store stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt

temperature.actual.input.dir will store stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt

Pressure Property file

stockholm_barometer_2013_2017.txt  will store   pressure.manual.input.dir

stockholmA_barometer_2013_2017.txt will store   pressure.automatic.input.dir

stockholm_barometer_1938_1960.txt  will store   pressure.1938.input.dir

stockholm_barometer_1862_1937.txt  will store  pressure.1862.input.dir

stockholm_barometer_1756_1858.txt  will store  pressure.1756.input.dir

stockholm_barometer_1859_1861.txt  will store  pressure.1859.input.dir

stockholm_barometer_1961_2012.txt  will store pressure.1961.input.dir

Build the jar using maven

mvn clean install

Upload the jar file built to the cluster driver node location

Spark Job Submission: 

Temperature Clean Up

spark-submit --class scala.spark.pressure --master yarn <path to weather-1.0.jar>

Pressure Clean Up

spark-submit --class scala.spark.Temperature --master yarn <path to weather-1.0.jar>

Git Code Checkin

cd local repository path

git init

git add .

git commit -m First commit

git remote add origin git hub url

git remote -v

git push origin master -f
