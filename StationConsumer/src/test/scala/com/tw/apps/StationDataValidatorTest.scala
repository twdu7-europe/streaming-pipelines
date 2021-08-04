package com.tw.apps

import com.tw.apps.StationDataTransformation.nycStationStatusJson2DF
import com.tw.apps.StationDataValidator._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

class StationDataValidatorTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status validation to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Validate nyc station data frame") {

      val testStationData = Seq(
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
          }""",
        """{
          "station_id":"84",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":null
          }""",
        """{
          "station_id":"85",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604
          }""")

      val expectedResult =
        Array(Row.fromSeq(Seq (19, 41, true, true, 1536242527, 83, "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)))

      Given("Sample data for station_status")
      val testDF = testStationData.toDF("raw_payload")

      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      print("DISPLAYING TRANSFORMED JSON DATA ***************************************************************")

      transformedDF.show()
      When("Validations are applied")
      val filteredResult = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      filteredResult equals expectedResult

//      DONE 1. Check in the file that there is a long
//      1.2     and lat for each station
//      2. Confirm that each station id is listed only once.
//      3. Bikes Available should be a non-negaitve number
//      4. Docks Available should be a non-negaitve number
    }
  }
}
