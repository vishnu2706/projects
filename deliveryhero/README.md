
# About Asssignment

- Receive as arguments the input path for the CSV file and an output path to save the results.
- Ingest the CSV file and store it in a place of your choice (Parquet file, Postgres, MySQL, MongoDB, etc).
- Query the stored data to generate two CSV reports:

   - `top_tipping_zones.csv`: Top 5 Dropoff Zones that pay the highest amount of tips.
   - `longest_trips_per_day.csv`: Top 5 longest trips per day of the first week of January 2018.

 Feel free to use any tool you are comfortable with for the data ingestion and transformation. Our only requirement is that the code is written in **Scala**, **Python** or **SQL**.
 The expected output for the reports is provided with this assignment. Be aware that some data cleanup is required to achieve the same result.
 For the zone names, please take a look at **Taxi Zone Lookup Table** in the taxi data website. It's up to you decide how to store / pre-load the lookup table to generate the report.
 The primary values for the code we look for are **simplicity, readability, testability, and maintainability**. It should be easy to scan the code, and rather quickly understand what it’s doing. Pay attention to naming and code style.

## prerequisites to run program in linux / mac

 * install Java 1.8
 * install scala 2.12
 * install hadoop v2 / v3
 * install Spark 2.4
 * install maven
 * prefer to use jetbrains intellij 2018 ide to run program

## procedures to follow before execute shell command

 * Build jar using maven ( command: mvn clean package)
 * copy the jar from target folder to some location <jar location>, jar name will be "hometest-0.1.jar"
 * download the depedency jars com.typesafe.config-1.2.1.jar, scopt_2.11-3.7.0.jar from maven repository
   and copy it to <jar location>
 * copy the raw file to location <row location>
 * copy look-up file to location <reference location>
 * create a folder to store output files <publish location>
 * run below command in linux/mac
 * sample output is available at the location "/projects/deliveryhero/src/test/data/output/"
   this output is generated as part of test case execution.
 * please check below column headers are available in row and lookup files.
   "tip_amount, tpep_dropoff_datetime, Borough, Zone, Trip_distance, PULocationID, LocationID"

## shell command to execute

 * fill the location tags properly and run below command.

spark-submit \
--master yarn \
--deploy-mode client \
--jars <jar location>/com.typesafe.config-1.2.1.jar, <jar location>/scopt_2.11-3.7.0.jar \
--class com.deliveryhero.TlcTripJob \
<jar location>/hometest-0.1.jar \
--source-path <row location> \
--lookup-path <reference location> \
--publish-path  <publish location>

 Example:

spark-submit \
--master yarn \
--deploy-mode client \
--jars <jar location>/com.typesafe.config-1.2.1.jar, <jar location>/scopt_2.11-3.7.0.jar \
--class com.deliveryhero.TlcTripJob \
<jar location>/hometest-0.1.jar \
--source-path /bigdata/projects/training/green_tripdata_2018-12.csv \
--lookup-path /bigdata/projects/training/lookup.csv \
--publish-path  /bigdata/projects/training/publish
