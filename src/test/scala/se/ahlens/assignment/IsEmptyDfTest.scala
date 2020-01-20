package se.ahlens.assignment

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IsEmptyDfTest extends AnyFunSuite with Matchers {
  test("Testing if DataFrame is empty or not...") {
    //val spark = SparkSession
      //.builder()
 //     .appName("DemoSparkApp")
      //.config("spark.some.config.option", "some-value")
      //.getOrCreate()

    val actual = Matches.readCsvToDF("match_skill.csv")

    assertResult(actual.head(1).isEmpty)(false)

  }
}
