package com.tw.apps

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object StationDataValidator {
  def filterValidData(data: DataFrame): Array[Row] ={
    val filtered = data
      .filter(col("latitude").isNotNull)
      .filter(col("longitude").isNotNull)
      .filter(col("bikes_available") >= 0)
      .filter(col("docks_available") >= 0)
      .withColumn("station_id_count", count("*").over(Window.partitionBy(col("station_id"))))
      .filter(col("station_id_count")===1).drop(col("station_id_count"))

    filtered.collect()
  }
}
