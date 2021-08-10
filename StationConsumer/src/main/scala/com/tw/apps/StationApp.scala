package com.tw.apps

import com.tw.apps.StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.{Dataset, SparkSession}

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

    val resultDF = nycStationDF
      .union(sfStationDF)
      .union(marsStationDF)
      .as[StationData]
      .groupByKey(r => r.station_id)
      .reduceGroups((r1, r2) => if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)

    resultDF
      .writeStream
      .foreachBatch((data: Dataset[StationData], batchId: Long) =>
        data
          .write
          .format("csv")
          .option("header", value = true)
          .option("truncate", value = false)
          .save(outputLocation)
      )
      .outputMode("complete")
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()

  }

}
