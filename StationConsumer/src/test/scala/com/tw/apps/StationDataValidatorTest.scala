package com.tw.apps

import com.tw.apps.StationDataBuilder.buildStationRow
import com.tw.apps.StationDataTransformation.nycStationStatusJson2DF
import com.tw.apps.StationDataValidator._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

//      2. Confirm that each station id is listed only once.
//      3. Bikes Available should be a non-negaitve number
//      4. Docks Available should be a non-negaitve number

class StationDataValidatorTest extends FeatureSpec with Matchers with GivenWhenThen {
  feature("Apply station status validation to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Validating missing longitude") {
      val testStationDataWithMissingLongitude = Seq(
        buildStationRow("1"),
        buildStationRow("2", longitude = "null")
      )

      val expectedResult = Array(
        Row.fromSeq(
          Seq(19, 41, true, true, 1536242527, "1", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)
        )
      )

      Given("Some rows are missing longitude")
      val testDF = testStationDataWithMissingLongitude.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validations are applied")
      val filteredResult = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult sameElements expectedResult)
    }

    scenario("Validating missing latitude") {
      val testStationDataWithMissingLatitude = Seq(
        buildStationRow("1"),
        buildStationRow("2", latitude = "null")
      )

      val expectedResult =
        Array(Row.fromSeq(Seq(19, 41, true, true, 1536242527, "1", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)))

      Given("Some rows are missing latitude")
      val testDF = testStationDataWithMissingLatitude.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validations are applied")
      val filteredResult = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult sameElements expectedResult)
    }
  }
}
