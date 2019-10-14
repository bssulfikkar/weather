## Loading Weather data into Hive table using scala and spark ##

1. **Data is available in the below urls**

**Temperature Data:** 
https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php


**Pressure Data:** 
https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php

temperatureconfig.properties and pressureconfig.properties files are created to keep the mapping of input files

2. **Temperature Input Path keys  are as below**

temperature.manual.input.dir

temperature.automatic.input.dir

temperature.space.input.dir

temperature.actual.input.dir

3. **Pressure Input Path keys are as below**

pressure.automatic.input.dir

pressure.manual.input.dir

pressure.1938.input.dir

pressure.1862.input.dir

pressure.1756.input.dir
 
pressure.1859.input.dir

pressure.1961.input.dir

4. **Temperature property file**

temperature.manual.input.dir  stores stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

temperature.automatic.input.dir  stores stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt

temperature.space.input.dir  stores stockholm_daily_temp_obs_1756_1858_t1t2t3.txt

temperature.actual.input.dir  stores stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt

temperature.actual.input.dir  stores stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt

5. **Pressure Property file**

stockholm_barometer_2013_2017.txt   stores   pressure.manual.input.dir

stockholmA_barometer_2013_2017.txt  stores   pressure.automatic.input.dir

stockholm_barometer_1938_1960.txt   stores pressure.1938.input.dir

stockholm_barometer_1862_1937.txt   stores  pressure.1862.input.dir

stockholm_barometer_1756_1858.txt   stores pressure.1756.input.dir

stockholm_barometer_1859_1861.txt   stores  pressure.1859.input.dir

stockholm_barometer_1961_2012.txt   stores pressure.1961.input.dir

6. **Build the jar using maven**

mvn clean install

7. **Upload the jar file built to the cluster driver node location**

**Spark Job Submission:** 

8. **Temperature Clean Up**

spark-submit --class scala.spark.pressure --master yarn <path to weather-1.0.jar>
 
9. **Pressure Clean Up**

spark-submit --class scala.spark.Temperature --master yarn <path to weather-1.0.jar>

10. **Git Code Checkin**

cd "local repository path"

git init

git add .

git commit -m First commit

git remote add origin "git hub url"

git remote -v

git push origin master -f
