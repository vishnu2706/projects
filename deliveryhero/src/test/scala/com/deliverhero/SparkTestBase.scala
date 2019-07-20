package com.deliveryhero

import org.apache.spark.sql.SparkSession

trait SparkTestBase   {

  implicit val spark = SparkSession.builder().master("local[*]").appName("test")
    .getOrCreate()
}