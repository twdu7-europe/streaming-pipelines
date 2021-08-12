package com.tw.apps

import StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
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

    nycStationDF
      .union(sfStationDF)
      .union(marsStationDF)
      .as[StationData]
      .groupByKey(r=>r.station_id)
      .reduceGroups((r1,r2)=>if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
      .withColumn("iso_timestamp",from_unixtime(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))
      .writeStream
      .format("overwriteCSV")
      .outputMode("complete")
      .option("header", true)
      .option("truncate", false)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputLocation)
      .start()
      .awaitTermination()

  }
}
