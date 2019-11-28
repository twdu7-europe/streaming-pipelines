#!/usr/bin/env bash
set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

function CitibikeApiProducer {
	java -jar ${DIR}/CitibikeApiProducer/build/libs/tw-citibike-apis-producer0.1.0.jar \
		--spring.profiles.active=$1 \
		--spring.profiles.active=local   
}

function rawDataSaver {
	spark-submit --conf spark.eventLog.enabled=true  \
		--class com.tw.apps.StationLocationApp \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
		${DIR}/rawDataSaver/target/scala-2.11/tw-raw-data-saver_2.11-0.0.1.jar 127.0.0.1:2181
}

function StationConsumer {
	spark-submit --class com.tw.apps.StationApp \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
		${DIR}/StationConsumer/target/scala-2.11/tw-station-consumer_2.11-0.0.1.jar \
		127.0.0.1:2181
}
