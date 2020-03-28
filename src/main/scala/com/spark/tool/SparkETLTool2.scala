/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.spark.tool
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ approx_count_distinct, expr, max, min, col, collect_list, lit, map, hash,concat }
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.to_json
import java.net.URI
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentMap }
import org.apache.hadoop.fs.{ FileSystem, Path, FileStatus, LocatedFileStatus, RemoteIterator }
/**
 * Usage: Aggregation [partitions] [numElem] [blockSize]
 */
object Aggregation2 {

  def readfile(path: String, spark: SparkSession): DataFrame = {
    if (path.endsWith("csv")) {
      return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

    }
    if (path.endsWith("parquet")) {
      return spark.read.parquet(path)

    }
    return null

  }
  
   def readfile3(path: String, spark: SparkSession): DataFrame = {
    if (path.endsWith("csv")) {
      return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

    }
    if (path.endsWith("parquet")) {
      return spark.read.parquet(path)

    }
    return null

  }


  def main(args: Array[String]) {
    val vars = new ConcurrentHashMap[String, String]
    args.filter(_.indexOf('=') > 0).foreach { arg =>
      val pos = arg.indexOf('=')
      vars.put(arg.substring(0, pos), arg.substring(pos + 1))
    }
    // initialize spark context
    val conf = new SparkConf().setAppName("fxconduct etl tool")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val path1 = vars.getOrDefault("path1", "C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\all\\2019-07-28.csv")
    val path2 = vars.getOrDefault("path2", "C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\all\\2019-07-29.csv")
    val keyCols = vars.getOrDefault("keyCols", "InvoiceNo,StockCode")
    val flatColumn = vars.getOrDefault("flatColumn", "")
    val outputPath = vars.getOrDefault("outputPath", "C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\etl\\" + System.currentTimeMillis() + "\\")
    val keyColumns = keyCols.split(",").toSeq.map(col(_));
    val t0 = System.currentTimeMillis()
    val oldDF = readfile(path1, spark).cache()
    val newDF = readfile(path2, spark).cache()
    System.out.println(oldDF.count)
    System.out.println(newDF.count)
    if (oldDF != null && newDF != null && !oldDF.rdd.isEmpty() && !newDF.rdd.isEmpty()) {
      var oldDF0 = oldDF.withColumn("uid", concat(keyColumns: _*)).withColumn("hash1", hash(oldDF.columns.map(col): _*))
      System.out.println("oldDF0:" + oldDF0.count)
      var newDF0 = newDF.withColumn("uid", concat(keyColumns: _*)).withColumn("hash2", hash(newDF.columns.map(col): _*))
      oldDF0.registerTempTable("table1")
      newDF0.registerTempTable("table2")
      // val fullout=spark.sql("SELECT * FROM table1 as t1 FULL OUTER JOIN table2 as t2 ON t1.uid = t2.uid")
      val deletedOrUpdated = spark.sql("SELECT * FROM table1 as t1 LEFT ANTI JOIN table2 as t2 ON t1.uid = t2.uid")
      //val changed=fullout.filter(col("hash1")=!=col("hash2"))
      deletedOrUpdated.show(100, false)
      System.out.println(deletedOrUpdated.count)

      val addedOrUpdated = spark.sql("SELECT * FROM table2 as t2 LEFT ANTI JOIN table1 as t1 ON t1.uid = t2.uid")
      //val changed=fullout.filter(col("hash1")=!=col("hash2"))
      addedOrUpdated.show(100, false)
      System.out.println(addedOrUpdated.count)
    }
    val t1 = System.currentTimeMillis()
    System.out.println("Total executing time is :" + (t1 - t0) / 1000)
    spark.stop()

    System.out.println("this is new line2")
     System.out.println("this is new line3 from master")

  }
}

 def readfile2(path: String, spark: SparkSession): DataFrame = {
    if (path.endsWith("csv")) {
      return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

    }
    if (path.endsWith("parquet")) {
      return spark.read.parquet(path)

    }
    return null

  }

 def readfile4(path: String, spark: SparkSession): DataFrame = {
    if (path.endsWith("csv")) {
      return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

    }
    if (path.endsWith("parquet")) {
      return spark.read.parquet(path)

    }
    return null

  }
// scalastyle:on println
