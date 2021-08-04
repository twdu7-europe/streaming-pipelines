package com.tw.apps

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StationDataValidator {
  def getData(data: DataFrame): DataFrame ={
    data
  }

  def filterValidData(data: DataFrame): Array[Row] ={
    val filtered = data
      .filter(col("latitude").isNotNull)
      .filter(col("longitude").isNotNull)
      .filter(col("bikes_available") >= 0)

    filtered.collect()
  }
}
