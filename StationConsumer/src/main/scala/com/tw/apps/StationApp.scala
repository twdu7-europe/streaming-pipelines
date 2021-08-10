package com.tw.apps

import com.tw.apps.StationDataTransformation._
import com.tw.apps.StationDataValidator.filterValidData
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_unixtime, last, window}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime}

import java.time.LocalDateTime

object StationApp {

  def main(args: Array[String]): Unit = {

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataNYC/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))
    val marsStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataMars/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/checkpointLocation"))

    val outputLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/dataLocation"))

    val invalidOutputLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/invalidDataLocation"))

    val spark = SparkSession.builder
      .appName("StationConsumer")
      .getOrCreate()

    import spark.implicits._

    val nycStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", nycStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("auto.offset.reset", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(nycStationStatusJson2DF(_, spark))

    val sfStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", sfStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("auto.offset.reset", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(sfStationStatusJson2DF(_, spark))

    val marsStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", marsStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("auto.offset.reset", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(marsStationStatusJson2DF(_, spark))

    val resultsDF = nycStationDF
      .union(sfStationDF)
      .union(marsStationDF)
      .as[StationData]
      .withColumn("iso_timestamp", from_unixtime(col("last_updated")).cast(TimestampType))
      .withWatermark("iso_timestamp", "1 minute")
      .groupBy(window(col("iso_timestamp"), "10 minutes"), col("station_id"))
      .agg(
        last(col("iso_timestamp")).alias("iso_timestamp"),
        last(col("bikes_available")).alias("bikes_available"),
        last(col("docks_available")).alias("docks_available"),
        last(col("is_renting")).alias("is_renting"),
        last(col("is_returning")).alias("is_returning"),
        last(col("last_updated")).alias("last_updated"),
        last(col("name")).alias("name"),
        last(col("latitude")).alias("latitude"),
        last(col("longitude")).alias("longitude")
      ).drop("window")

    resultsDF
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .foreachBatch((batchDF: DataFrame, batchId: Long) => {
          batchDF.persist()
          val (valid: DataFrame, invalid: DataFrame) = filterValidData(batchDF)
          valid
            .write
            .format("csv")
            .mode(SaveMode.Append)
            .option("header", true)
            .option("truncate", false)
            .option("path", outputLocation)
            .save()

        invalid
          .write
          .format("csv")
          .mode(SaveMode.Append)
          .option("header", true)
          .option("truncate", false)
          .option("path", invalidOutputLocation)
          .save()

          batchDF.unpersist()
      })
      .start()
      .awaitTermination()

  }
}
