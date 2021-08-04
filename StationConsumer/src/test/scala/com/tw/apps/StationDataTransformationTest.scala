package com.tw.apps

import com.tw.apps.StationDataTransformation.{marsStationStatusJson2DF, nycStationStatusJson2DF}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._

class StationDataTransformationTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Transform nyc station data frame") {

      val testStationData =
        """{
          "station_id":"83",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }"""

      val schema = ScalaReflection.schemaFor[StationData].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(nycStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(19)
      row1.get(1) should be(41)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1536242527)
      row1.get(5) should be("83")
      row1.get(6) should be("Atlantic Ave & Fort Greene Pl")
      row1.get(7) should be(40.68382604)
      row1.get(8) should be(-73.97632328)
    }

    scenario("Transform marseille station data") {
          val testDataMarseille =
                """{"payload": {"network": {
            "company": [
              "JCDecaux"
            ],
            "gbfs_href": "https://gbfs.fordgobike.com/gbfs/gbfs.json",
            "href": "/v2/networks/le-velo",
            "id": "le-velo",
            "location": {
                  "city": "Marseille",
                  "country": "FR",
                  "latitude": 43.296482,
                  "longitude": 5.36978
                },
            "name": "Le vélo",
            "source": "https://developer.jcdecaux.com",
            "stations": [
              {
                    "empty_slots": 14,
                    "extra": {
                      "address": "391 MICHELET - 391 BOULEVARD MICHELET",
                      "banking": true,
                      "bonus": false,
                      "last_update": 1628003594000,
                      "slots": 19,
                      "status": "OPEN",
                      "uid": 8149
                    },
                    "free_bikes": 5,
                    "id": "686e48654a218c70daf950a4e893e5b0",
                    "latitude": 43.25402727813068,
                    "longitude": 5.401873594694653,
                    "name": "8149-391 MICHELET",
                    "timestamp": "2021-08-03T15:18:38.421000Z"
                  },
                  {
                    "empty_slots": 10,
                    "extra": {
                      "address": "TEISSEIRE ROUBAUD - ANGLE RUE RAYMOND TEISSEIRE ET RUE MAGUY ROUBAUD",
                      "banking": true,
                      "bonus": false,
                      "last_update": 1628003798000,
                      "slots": 10,
                      "status": "OPEN",
                      "uid": 9207
                    },
                    "free_bikes": 0,
                    "id": "259c5e65f9e2af98046069144c666aa6",
                    "latitude": 43.27252367086013,
                    "longitude": 5.399686062414487,
                    "name": "9207- TEISSEIRE - ROUBAUD",
                    "timestamp": "2021-08-03T15:18:38.428000Z"
                  }
            ]
          }}}"""

          Given("Sample data for status_information")
          val testDF1 = Seq(testDataMarseille).toDF("raw_payload")

          When("Transformations are applied")
          val resultDF1: DataFrame = testDF1.transform(marsStationStatusJson2DF(_, spark))

          Then("Useful columns are extracted")
          resultDF1.schema.fields(0).name should be("bikes_available")
          resultDF1.schema.fields(0).dataType.typeName should be("integer")
          resultDF1.schema.fields(1).name should be("docks_available")
          resultDF1.schema.fields(1).dataType.typeName should be("integer")
          resultDF1.schema.fields(2).name should be("is_renting")
          resultDF1.schema.fields(2).dataType.typeName should be("boolean")
          resultDF1.schema.fields(3).name should be("is_returning")
          resultDF1.schema.fields(3).dataType.typeName should be("boolean")
          resultDF1.schema.fields(4).name should be("last_updated")
          resultDF1.schema.fields(4).dataType.typeName should be("long")
          resultDF1.schema.fields(5).name should be("station_id")
          resultDF1.schema.fields(5).dataType.typeName should be("string")
          resultDF1.schema.fields(6).name should be("name")
          resultDF1.schema.fields(6).dataType.typeName should be("string")
          resultDF1.schema.fields(7).name should be("latitude")
          resultDF1.schema.fields(7).dataType.typeName should be("double")
          resultDF1.schema.fields(8).name should be("longitude")
          resultDF1.schema.fields(8).dataType.typeName should be("double")

          val row1: Row = resultDF1.head()
          row1.get(0) should be(5)
          row1.get(1) should be(14)
          row1.get(2) shouldBe true
          row1.get(3) shouldBe true
          row1.get(4) should be(1536242527)
          row1.get(5) should be("le-velo")
          row1.get(6) should be("Le vélo")
          row1.get(7) should be(43.25402727813068)
          row1.get(8) should be(5.401873594694653)
    }
  }
}
