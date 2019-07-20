package com.deliveryhero.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object FileSystemUtil {

  private val log = LogManager.getLogger(this.getClass)

  /**
    * Check if a given path exists on HDFS
    */
  def exists(path: String)(implicit spark: SparkSession): Boolean = {
    log.info(s"Cheking if $path exists")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  def fileRenaming(path: String, filename: String)(implicit spark: SparkSession) {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val file = fs.globStatus(new Path(s"${path}/part*"))(0).getPath().getName()
    fs.rename(new Path(s"${path}/$file"), new Path(s"$path/$filename"))
  }
}