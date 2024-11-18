// Databricks notebook source
val catalog = "chengwang611_ws_2025"
val schema = "default"
val volume = "temp"
val downloadUrl = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
val fileName = "rows.csv"
val tableName = "<table_name>"
val pathVolume = s"/Volumes/$catalog/$schema/$volume"
val pathTable = s"$catalog.$schema"
println(pathVolume) // Show the complete path
println(pathTable) // Show the complete path

// COMMAND ----------

// MAGIC %md
// MAGIC here i will do the dataframe

// COMMAND ----------

// COMMAND ----------

dbutils.fs.cp(downloadUrl, s"$pathVolume/$fileName")
val data = Seq((2021, "test", "Albany", "M", 42))
val columns = Seq("Year", "First_Name", "County", "Sex", "Count")

val df1 = data.toDF(columns: _*)

display(df1) // The display() method is specific to Databricks notebooks and provides a richer visualization.
// df1.show() The show() method is a part of the Apache Spark DataFrame API and provides basic visualization.
