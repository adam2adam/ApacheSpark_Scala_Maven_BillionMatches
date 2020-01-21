package se.ahlens.assignment

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.reflect.io.File

object Matches {

  def main(args: Array[String]) {
    // Check arguments
    if (args.length != 3) {
      println("Please give three input parameters (file names)... ")
      return
    }
    // Check filenames
    else if (!isFileExisting(args(0)) || !isFileExisting(args(1)) || !isFileExisting(args(2))){
      println("One or more files missed! Please check filenames... ")
      return
    }

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("DemoSparkApp")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("DemoSparkApp")
      .getOrCreate()

    // Create DataFrames from files after cleaning from NaNs
    val dfMatchSkills = readFileToDfAndClean(spark, args(0))
    val dfMatchSmall = readFileToDfAndClean(spark, args(1))
    val dfPlayerMatchSmall = readFileToDfAndClean(spark, args(2))

    //Prepare data and export output files
    prepareAndExport(dfMatchSkills, dfMatchSmall, dfPlayerMatchSmall)
    spark.stop()
  }

  //Prepare data and export output files
  private def prepareAndExport(dfMatchSkillsCal: DataFrame, dfMatchSmallCal: DataFrame, dfPlayerMatchSmallCal: DataFrame): Unit = {
    // Select required columns
    val dfMatchSkills1 = dfMatchSkillsCal.select("match_id", "skill")

    // Select required columns
    val dfPlayerMatchSmall1 = dfPlayerMatchSmallCal
      .select("match_id", "gold_per_min", "account_id")

    // Required castings and calculations for the result.
    // Then select required columns
    val dfMatchSmall2 = dfMatchSmallCal.withColumn("start_time1", from_unixtime(col("start_time")))
      .withColumn("end_time", from_unixtime(col("start_time").cast(LongType) + col("duration").cast(LongType)))
      .withColumn("score", ((col("tower_status_radiant").cast(IntegerType) - col("tower_status_dire").cast(IntegerType)) + (col("barracks_status_radiant").cast(IntegerType) - col("barracks_status_dire").cast(IntegerType))) / col("human_players").cast(IntegerType))
      .select("match_id", "match_seq_num", "start_time1", "end_time", "score")
      .withColumnRenamed("start_time1", "start_time")

    dfMatchSmall2.show()

    // Join different pairs of tables in all possibilities --> Not empty
    dfPlayerMatchSmall1.join(dfMatchSmall2, "match_id").show()
    dfMatchSkills1.join(dfPlayerMatchSmall1, "match_id").show()
    val dfResults4TWO = dfMatchSkills1.join(dfMatchSmall2, "match_id")
    dfResults4TWO.show()

    // Join 3 tables --> Empty
    val dfResult = dfMatchSkills1
      .join(dfPlayerMatchSmall1, "match_id")
      .join(dfMatchSmall2, "match_id")
      .orderBy(desc("skill"))
    dfResult.show()

    // Export DataFrame of joined 3 tables --> Empty
    dfResult.coalesce(1)
      .write
      .format("parquet")
      .option("header", "true")
      .mode("overwrite")
      .save("output/output_1file.parquet")
 /*
    // If you want to create a csv file
    dfResult.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save("output/output_1file.csv")
*/
    // Export DataFrame of joined 2 tables --> Not empty
    dfResults4TWO.coalesce(1)
      .write
      .format("parquet")
      .option("header", "true")
      .mode("overwrite")
      .save("output/output4Two_1file.parquet")
  }

  // Create DataFrames from files after cleaning from NaNs
  private def readFileToDfAndClean(spark: SparkSession, fileName: String): DataFrame = {
    val df: DataFrame = readCsvToDF(spark, fileName)
    println(df.count())
    df.show(10)
    df.printSchema()
    val df2 = nanCleanse(df, df.columns)
    df2
  }

  // Create DataFrames from files
  def readCsvToDF(spark: SparkSession, fileName: String): DataFrame = {
    val newDF = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") //first line in file has headers
      .option("inferschema", "true")
      .load(fileName)
    newDF
  }

  // Cleaning DataFrames from NaNs
  def nanCleanse(df: DataFrame, columns: Array[String]): DataFrame = {
    val retDF = df.na.fill(0, columns)
    retDF
  }

  // Checking if files exist
  def isFileExisting(fileName: String): Boolean = {
    val file = File(fileName)
    if (file.isFile && file.exists) true else false
  }

}