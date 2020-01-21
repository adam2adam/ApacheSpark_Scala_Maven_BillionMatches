package se.ahlens.assignment

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NumOfColumnsTest extends AnyFunSuite with Matchers with DataFrameSuiteBase   {
  test("Testing number of columns of a match_skill dataframe") {

    val expectedNumOfColumns = 2
    val actual = Matches.readCsvToDF(spark, "src/test/resources/data/match_skill_1000.csv")

    assert(actual.columns.length === expectedNumOfColumns)
  }

}
