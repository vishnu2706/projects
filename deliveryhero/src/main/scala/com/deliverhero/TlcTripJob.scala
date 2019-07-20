package com.deliveryhero

import java.io.{PrintWriter, StringWriter}

import com.deliveryhero.model.TlcCmdConfig
import com.deliveryhero.util.FileSystemUtil

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.control.NonFatal

object TlcTripJob {

  val log: Logger = LogManager.getLogger("TlcTripJob")
  val conf: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {

    val dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = "yyyy-MM-dd"
    val dateTimeCondition = "2018-01-08 00:00:00"
    val dropOffCol = "tpep_dropoff_datetime"
    val partCol = "dropoff_date"
    val tripDistCol = "trip_distance"
    val rankCol = "rank"
    val yearVal = "2018"

    //read application config
    val tripPublishColumns = conf.getString("trip.publish.column.list").split(",").toSeq
    val tipPublishColumns = conf.getString("tip.publish.column.list").split(",").toSeq

    //validating options based on parser logic
    val cmd = getCmdLineArguments(args)
    val tipPublish = s"${cmd.publishPath}/tip/"
    val tripPublish = s"${cmd.publishPath}/trip/"

    //instance creation of spark specific contexts
    implicit val spark = SparkSession.builder()
      .appName(s"Tlc data load ${cmd.sourcePath}")
      .getOrCreate()

    import spark.implicits._

    println(s"source path: ${cmd.sourcePath}")
    println(s"publish path: ${cmd.publishPath}")

    //die if the input paths not exists
    if (!FileSystemUtil.exists(cmd.sourcePath))
      throw new IllegalStateException(s"file ${cmd.sourcePath} not exists.")

    //reading row csv file and ignoring white spaces
    val dfRead = spark.read.format("csv").option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true).option("header", true)

    // applying format specific options
    val dfRow = if (cmd.csvDelimiter.isDefined) dfRead.option("delimiter", cmd.csvDelimiter.get) else dfRead
    val dfAll = dfRow.load(cmd.sourcePath)

    //reading lookup data
    val lookupDf = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true).csv(cmd.lookupPath)

    val std = try {
      val partition = Window.partitionBy(partCol).orderBy(desc(tripDistCol))
      val columns = dfAll.columns
        .map(col => {
          col
        }).toSeq ++ lookupDf.columns.map(col => {
        col
      })

      // left joining row data and look up data using the key LocationID and PULocationID
      val joinDf = dfAll.join(lookupDf, dfAll("PULocationID") === lookupDf("LocationID"), "left_outer").
        select(columns.head, columns.tail: _*)

      // logic for Top 5 longest trips per day of the first week of January 2018
      val df1 = joinDf.withColumn(partCol, from_unixtime(unix_timestamp(col(dropOffCol), dateTimeFormat), dateFormat))
      val df2 = df1.filter(year(col(partCol)) === lit(yearVal) and unix_timestamp(col(dropOffCol), dateTimeFormat) < unix_timestamp(lit(dateTimeCondition), dateTimeFormat))
      val df3 = df2.withColumn(rankCol, row_number().over(partition))
      val longestTripDf = df3.filter(col(rankCol) < 6).orderBy(desc(partCol))

      // logic for Top 5 Dropoff Zones that pay the highest amount of tips
      val topTippingDf = joinDf.groupBy("Borough", "Zone").agg(sum("tip_amount") as "total_tip_amount")
        .orderBy(desc("total_tip_amount")).limit(5)

      // writing output to file system
      longestTripDf.select(tripPublishColumns.head, tripPublishColumns.tail: _*).coalesce(1).write
        .option("header", "true").mode(SaveMode.Overwrite).csv(tripPublish)
      topTippingDf.select(tipPublishColumns.head, tipPublishColumns.tail: _*).coalesce(1).write
        .option("header", "true").mode(SaveMode.Overwrite).csv(tipPublish)

      // renaming files
      FileSystemUtil.fileRenaming(tripPublish, "longest_trips_per_day.csv")
      FileSystemUtil.fileRenaming(tipPublish, "top_tipping_zones.csv")
    }
    catch {
      case NonFatal(e) =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        throw e
    }
  }

  def getCmdLineArguments(args: Array[String]): TlcCmdConfig = {

    val parser = new scopt.OptionParser[TlcCmdConfig]("spark-submit [spark options] deliveryhero.jar") {
      head("\nTlcTest", "")
      var rawFormat: Option[String] = None
      opt[String]('S', "source-path").required().action((value, config) =>
        config.copy(sourcePath = value)).text("please provide the source file path")
      opt[String]('P', "publish-path").required().action((value, config) =>
        config.copy(publishPath = value)).text("please provide the publish path")
      opt[String]('L', "lookup-path").required().action((value, config) =>
        config.copy(lookupPath = value)).text("please provide the lookup path")
      opt[String]("delimiter").optional().action((value, config) =>
        config.copy(csvDelimiter = Some(value))).text("use the specific delimiter instead of ',' for CSV format")

      help("help").text("prints this usage text")
    }

    val optionCmd = parser.parse(args, TlcCmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }
}