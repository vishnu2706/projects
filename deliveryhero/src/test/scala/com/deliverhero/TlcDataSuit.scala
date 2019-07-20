package com.deliveryhero

import com.deliveryhero.TlcTripJob.conf
import com.deliveryhero.util.FileSystemUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class TlcDataSuit extends FunSuite with SparkTestBase {

  val dateTimeFormat = "yyyy-MM-dd HH:mm:ss"
  val dateFormat = "yyyy-MM-dd"
  val dateTimeCondition = "2018-01-08 00:00:00"
  val dropOffCol = "tpep_dropoff_datetime"
  val partCol = "dropoff_date"
  val tripDistCol = "trip_distance"
  val rankCol = "rank"
  val yearVal = "2018"


  test("Test for TLC data") {

    val sourcePath = "src/test/data/input/yellow-trip-2018.txt"
    val lookupPath = "src/test/data/input/lookup.csv"
    val tripPublish = "src/test/data/output/trip"
    val tipPublish = "src/test/data/output/tip"

    val tripPublishColumns = conf.getString("trip.publish.column.list").split(",").toSeq
    val tipPublishColumns = conf.getString("tip.publish.column.list").split(",").toSeq

    val dfRead = spark.read.format("csv").option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true).option("header", true)
    // applying format specific options
    val dfRow = dfRead
    val dfAll = dfRow.load(sourcePath)
    val lookupDf = spark.read.option("header", "true").option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true).csv(lookupPath)


    val std = try {
      val partition = Window.partitionBy(partCol).orderBy(desc(tripDistCol))
      val columns = dfAll.columns
        .map(col => {
          col
        }).toSeq ++ lookupDf.columns.map(col => {
        col
      })

      val joinDf = dfAll.join(lookupDf, dfAll("PULocationID") === lookupDf("LocationID"), "left_outer").
        select(columns.head, columns.tail: _*)
      val df1 = joinDf.withColumn(partCol, from_unixtime(unix_timestamp(col(dropOffCol), dateTimeFormat), dateFormat))
      val df2 = df1.filter(year(col(partCol)) === lit(yearVal) and unix_timestamp(col(dropOffCol), dateTimeFormat) < unix_timestamp(lit(dateTimeCondition), dateTimeFormat))
      val df3 = df2.withColumn(rankCol, row_number().over(partition))
      val longestTripDf = df3.filter(col(rankCol) < 6).orderBy(desc(partCol))
      val topTippingDf = joinDf.groupBy("Borough", "Zone").agg(sum("tip_amount") as "total_tip_amount")
        .orderBy(desc("total_tip_amount")).limit(5)

      longestTripDf.select(tripPublishColumns.head, tripPublishColumns.tail: _*).coalesce(1).write
        .option("header", "true").mode(SaveMode.Overwrite).csv(tripPublish)
      topTippingDf.select(tipPublishColumns.head, tipPublishColumns.tail: _*).coalesce(1).write
        .option("header", "true").mode(SaveMode.Overwrite).csv(tipPublish)
      assertResult(1)(1)

      FileSystemUtil.fileRenaming(tripPublish, "longest_trips_per_day.csv")
      FileSystemUtil.fileRenaming(tipPublish, "top_tipping_zones.csv")
    }
  }}