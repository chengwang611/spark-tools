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
import org.apache.spark.sql.functions.{ approx_count_distinct, expr, max, min, col, collect_list, lit, map,to_date,trim }
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.functions.to_json
import java.net.URI
import java.io.File
import java.util.concurrent.{ ConcurrentHashMap, ConcurrentMap }
import org.apache.hadoop.fs.{ FileSystem, Path, FileStatus, LocatedFileStatus, RemoteIterator }
/**
 * Usage: Aggregation  path1=/tmp/fxconduct/2019-07-28.csv path2=/tmp/fxconduct/2019-07-28.csv keyCols=InvoiceNo,StockCode flatColumn=Description outputPath=/tmp/fxconduct/output/ outputFormat=csv
 */
object Aggregation {

  def readfile(path: String, spark: SparkSession): DataFrame = {
    if (path.endsWith("csv")) {
      return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

    }
    if (path.endsWith("parquet")) {
      return spark.read.parquet(path)

    }
    return null

  }
  
  def writeFile(df:DataFrame,path:String,partition:Int)  {
    if(path.endsWith("csv"))
      df.repartition(partition).write.format("csv").mode("overwrite").option("header", "true").option("sep", ",").save(path)
    else if(path.endsWith("parquet"))
      df.repartition(partition).write.parquet(path)
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
    val path1 = vars.getOrDefault("path1", new File("data/input/2019-07-28.csv").getAbsolutePath())
    val path2 = vars.getOrDefault("path2", new File("data/input/2019-07-29.csv").getAbsolutePath())
    val keyCols = vars.getOrDefault("keyCols", "InvoiceNo,StockCode")
    val flatColumn = vars.getOrDefault("flatColumn", "")
    val outputPath = vars.getOrDefault("outputPath", new File("data/output").getAbsolutePath()+"\\" + System.currentTimeMillis() + "\\")
    val outputFormat=vars.getOrDefault("outputFormat", "parauet")
    val keyColumns = keyCols.split(",").toSeq.map(col(_));
    val t0=System.currentTimeMillis()
    val oldDF = readfile(path1, spark)//.cache()
    val newDF = readfile(path2, spark)//.cache()
    if (oldDF != null && newDF != null && !oldDF.rdd.isEmpty() && !newDF.rdd.isEmpty()) {
      var oldDF0 = oldDF.withColumn("uid", concat(keyColumns: _*))
      var newDF0 = newDF.withColumn("uid", concat(keyColumns: _*))
      if (!"".equalsIgnoreCase(flatColumn)) {
        oldDF0 = oldDF0.withColumn(flatColumn + "_json", to_json(col(flatColumn))).drop(flatColumn)
        newDF0 = newDF0.withColumn(flatColumn + "_json", to_json(col(flatColumn))).drop(flatColumn)
      }
     // oldDF0.cache()
     // newDF0.cache()
      val t2=System.currentTimeMillis()
      val deletedOrUpdateUid = oldDF0.except(newDF0).select("uid")
      System.out.println("deletedOrUpdate :")
      oldDF0.except(newDF0).show(false)
      val addedOrUpdateUid = newDF0.except(oldDF0).select("uid")
      val t3=System.currentTimeMillis()
      
      //System.out.println("add OrUpdate :")
      //newDF0.except(oldDF0).show(false)
      //get ids for update,add and delete
      val updateUid = deletedOrUpdateUid.intersect(addedOrUpdateUid).withColumnRenamed("uid", "uuid")
      val deletedUid = deletedOrUpdateUid.withColumnRenamed("uid", "uuid").except(updateUid)
      val addedUid = addedOrUpdateUid.withColumnRenamed("uid", "uuid").except(updateUid)
      //fetch the raw dataframe
      val oldDFwithUid = oldDF.withColumn("uid", concat(keyColumns: _*))
      val newDFwithUid = newDF.withColumn("uid", concat(keyColumns: _*))

      //deleted records in raw file
      val deletedDF = oldDFwithUid.join(deletedUid, oldDFwithUid.col("uid") === deletedUid.col("uuid"))
      System.out.println("deleted:")
      deletedDF.show(false)
      writeFile(deletedDF.drop("uid").drop("uuid"),outputPath + "deleted."+outputFormat,1)
      //deletedDF.drop("uid").drop("uuid").repartition(1).write.parquet(outputPath + "deleted.parquet")

      //added records in raw
      val addedDF = newDFwithUid.join(addedUid, newDFwithUid.col("uid") === addedUid.col("uuid"))
      System.out.println("added:")
      addedDF.show(false)
      writeFile(deletedDF.drop("uid").drop("uuid"),outputPath + "added."+outputFormat,1)
      //addedDF.drop("uid").drop("uuid").repartition(1).write.parquet(outputPath + "added.parquet")
      //updated records in the new file

      val updateNewDF = newDFwithUid.join(updateUid, newDFwithUid.col("uid") === updateUid.col("uuid"))
      System.out.println("update as :")
      updateNewDF.show(false)
      writeFile(deletedDF.drop("uid").drop("uuid"),outputPath + "updated_as."+outputFormat,1)
      //updateNewDF.drop("uid").drop("uuid").repartition(1).write.parquet(outputPath + "updated_as.parquet")
      //updated records in the old file

      val updateOldDF = oldDFwithUid.join(updateUid, oldDFwithUid.col("uid") === updateUid.col("uuid"))
      System.out.println("updated from :")
      updateOldDF.show(false)

      val noChange_left_anti = newDFwithUid.join(addedOrUpdateUid, newDFwithUid.col("uid") === addedOrUpdateUid.col("uid"), "left_anti")
      .withColumn("extracted_date", to_date(regexp_extract(col("InvoiceDate"),"[0-9]{2}/[0-9]{1}/[0-9]{4}",0),"MM/dd/yyyy"))
      .withColumn("extracted_name", regexp_extract(col("Description"),"(BOXES)",0)).filter(trim(col("extracted_name"))==="BOXES")
      System.out.println("left_anti -no change :")
      noChange_left_anti.show(false)
      System.out.println("old  =" + oldDF.count)
      System.out.println("new  =" + newDF.count)
      System.out.println("deleted =" + deletedDF.count)
      System.out.println("added =" + addedDF.count)
      System.out.println("update new =" + updateNewDF.count)
      System.out.println("update old  =" + updateOldDF.count)
      //      System.out.println("no change =" + noChange.count)
      System.out.println(" left-anti-no change =" + noChange_left_anti.count)

    }
    val t1=System.currentTimeMillis()
    System.out.println("Total executing time is :"+(t1-t0)/1000)
   
    spark.stop()
    System.out.println("this is new line2")
    System.out.println("this is new line3 from master")
     System.out.println("this is new line3 from master changed")
  }
}
// scalastyle:on println
