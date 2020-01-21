
#  Billion Matches project with Scala Maven on Apache Spark

**There are two different versions of this project (Both of them are runnable):**

* **Project for local run:** you can clone current repository  
* **Databricks project link:**  [https://databricks-prod-cloudfront.cloud.databricks.com/...](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8541621492736890/671524711240630/2868629089952293/latest.html)

##### @version v1.0.0

## Functionality

-	Maven as build tool
-	Apache Spark as data processing engine
-	Input files can be found here under the "Samples" section: [https://blog.opendota.com/2017/03/24/datadump2](https://blog.opendota.com/2017/03/24/datadump2/)
-	Input file names: *match_skill.csv, matches_small.csv, player_matches_small.csv*
-	Output file format: Parquet
-	Scalatest library is used for testing
-	".jar" file location: "target"

## Notes
- It's easier to test with smaller input files
- Sample files (1000 records) can be found under "resources/data": *match_skill_1000.csv, matches_small_1000.csv, player_matches_small_1000.csv*


## Author

* **Adem Sezer**

