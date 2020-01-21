package se.ahlens.assignment

import com.holdenkarau.spark.testing._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EmptyDFTest extends AnyFunSuite with Matchers with DataFrameSuiteBase  {
  test("Testing if DataFrame is empty or not...") {

    val dfSkill = Matches.readCsvToDF(spark, "src/test/resources/data/match_skill_1000.csv")

    assertResult(dfSkill.head(1).isEmpty)(false)

  }
}
