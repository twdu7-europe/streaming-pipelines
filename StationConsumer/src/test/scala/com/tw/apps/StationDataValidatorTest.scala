package com.tw.apps

import com.tw.apps.StationDataBuilder.buildStationRow
import com.tw.apps.StationDataTransformation.nycStationStatusJson2DF
import com.tw.apps.StationDataValidator._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

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
      val (filteredResult, _) = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult.collect() sameElements expectedResult)
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
      val (filteredResult, _) = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult.collect() sameElements expectedResult)
    }

    scenario("Validating bikes available is a non-negative number") {
      val testStationDataWithMissingLatitude = Seq(
        buildStationRow("1", bikesAvailable = 0),
        buildStationRow("2", bikesAvailable = -3)
      )

      val expectedResult =
        Array(Row.fromSeq(Seq(0, 41, true, true, 1536242527, "1", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)))

      Given("Some rows have negative bikes available")
      val testDF = testStationDataWithMissingLatitude.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validations are applied")
      val (filteredResult, _) = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult.collect() sameElements expectedResult)
    }

    scenario("Validating docks available is a non-negative number") {
      val testStationDataWithMissingLatitude = Seq(
        buildStationRow("1", docksAvailable = 0),
        buildStationRow("2", docksAvailable = -3)
      )

      val expectedResult =
        Array(Row.fromSeq(Seq(19, 0, true, true, 1536242527, "1", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)))

      Given("Some rows have negative docks available")
      val testDF = testStationDataWithMissingLatitude.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validations are applied")
      val (filteredResult, _) = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult.collect() sameElements expectedResult)
    }

    scenario("Validating station ids are listed once") {
      val testStationDataWithMissingLatitude = Seq(
        buildStationRow("1"),
        buildStationRow("2"),
        buildStationRow("2")
      )

      val expectedResult =
        Array(Row.fromSeq(Seq(19, 41, true, true, 1536242527, "1", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328)))

      Given("Some rows have the same station id")
      val testDF = testStationDataWithMissingLatitude.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validations are applied")
      val (filteredResult, _) = filterValidData(transformedDF)

      Then("Only valid rows are retrieved")
      assert(filteredResult.collect() sameElements expectedResult)
    }
  }

  feature("Return the invalid data") {

    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Should return empty array when there is no invalid data") {
      val testValidStationData = Seq(
        buildStationRow("1"),
        buildStationRow("2")
      )

      val expectedResult = Array.empty[Row]

      Given("We have only valid data")
      val testDF = testValidStationData.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validation is applied")
      val (_, filteredInvalidData) = filterValidData(transformedDF)

      Then("Empty array should be returned")
      assert(filteredInvalidData.collect() sameElements expectedResult)
    }

    scenario("Should return only invalid data") {
      val invalidData = buildStationRow("2", latitude = null)

      val testValidAndInvalidStationData = Seq(
        buildStationRow("1"),
        invalidData
      )

      val expectedResult = Seq(invalidData).toDF("raw_payload").transform(nycStationStatusJson2DF(_, spark)).collect()

      Given("We have only valid and invalid data")
      val testDF = testValidAndInvalidStationData.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validation is applied")
      val (_, filteredInvalidData) = filterValidData(transformedDF)

      Then("Only invalid data should be returned")
      assert(filteredInvalidData.collect() sameElements expectedResult)
    }

    scenario("Should return as invalid data when having duplicated station_id") {

      val testValidAndInvalidStationData = Seq(
        buildStationRow("1"),
        buildStationRow("2"),
        buildStationRow("2")
      )

      val expectedData = Seq(
        buildStationRow("2"),
        buildStationRow("2")
      )

      val expectedResult = expectedData.toDF("raw_payload").transform(nycStationStatusJson2DF(_, spark)).collect()

      Given("We have only valid and invalid data with duplicated station_ids")
      val testDF = testValidAndInvalidStationData.toDF("raw_payload")
      val transformedDF = testDF.transform(nycStationStatusJson2DF(_, spark))

      When("Validation is applied")
      val (_, filteredInvalidData) = filterValidData(transformedDF)

      Then("Only invalid data should be returned")
      assert(filteredInvalidData.collect() sameElements expectedResult)
    }

  }
}
