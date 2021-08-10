package com.tw.apps

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object StationDataValidator {
  def filterValidData(data: DataFrame): (DataFrame, DataFrame) = {

    val validation = col("latitude").isNotNull &&
      col("longitude").isNotNull &&
      col("bikes_available") >= 0 &&
      col("docks_available") >= 0 &&
      col("station_id_count") === 1

    val valid = data
      .withColumn("station_id_count", count("*").over(Window.partitionBy(col("station_id"))))
      .filter(validation)
      .drop(col("station_id_count"))

    val invalid = data
      .withColumn("station_id_count", count("*").over(Window.partitionBy(col("station_id"))))
      .filter(not(validation))
      .drop(col("station_id_count"))


    (valid, invalid)
  }
}
