package com.tw.apps

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}


object CombineSmallFilesApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      val message = "Two arguments are required: \"zookeeper server\" and \"application folder in zookeeper\"!"
      throw new IllegalArgumentException(message)
    }
    val zookeeperConnectionString = args(0)
    val zookeeperFolder = args(1)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val smallFilesLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))

    val aggregatedFilesLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))

    val spark = SparkSession.builder
      .appName("SmallFileAggregator")
      .getOrCreate()

    val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val directorySize = fs.getContentSummary(new Path("/path/path")).getLength

    val blockSize = spark.sparkContext.hadoopConfiguration.get("dfs.blocksize")
    val repartitionFactor=Math.ceil(directorySize/blockSize).toInt

    val smallFiles = spark.read.parquet("path", smallFilesLocation)
    smallFiles.repartition(repartitionFactor)
      .write
      .parquet(aggregatedFilesLocation)
  }

}
