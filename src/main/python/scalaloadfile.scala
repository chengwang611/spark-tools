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
