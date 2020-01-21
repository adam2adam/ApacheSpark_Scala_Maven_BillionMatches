package se.ahlens.assignment

import com.holdenkarau.spark.testing._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ReadCsvToDfTest extends AnyFunSuite with Matchers with DataFrameSuiteBase {

  test("Testing readCsvToDF function...") {

    val expectedSchema = List(
      StructField("match_id", LongType, nullable = false),
      StructField("skill", IntegerType, nullable = false)
    )

    val expectedData = Seq(
      Row(1971358627, 1),
      Row(1620277838, 2),
      Row(1967118934, 1)
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val dfSkill = Matches.readCsvToDF(spark, "src/test/resources/data/match_skill_1000.csv")

    assertResult(dfSkill.columns)(expectedDF.columns)

  }
}