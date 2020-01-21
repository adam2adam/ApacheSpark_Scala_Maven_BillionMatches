package se.ahlens.assignment

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NumOfColumnsTest extends AnyFunSuite with Matchers {
  test("Testing number of columns of a dataframe") {

    val expectedNumOfColumns = 2
    val actual = Matches.readCsvToDF("src/test/resources/data/match_skill_1000.csv")

    assert(actual.columns.length === expectedNumOfColumns)
  }

}
