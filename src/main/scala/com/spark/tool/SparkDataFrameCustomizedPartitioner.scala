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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ approx_count_distinct, expr, max, min, col }
import java.net.URI
import org.apache.hadoop.fs.{ FileSystem, Path, FileStatus, LocatedFileStatus, RemoteIterator }
import org.apache.spark.Partitioner

class DomainNamePartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  val r = scala.util.Random
 // val r2 = scala.util.Random
  override def getPartition(key: Any): Int = {
    var code =1;
    if(key==null){
     code= r.nextInt(50)
    }
    else{
     code  =(key.hashCode % (numPartitions-50)+50)//r2.nextInt(6)+3// 
    }
    code
  }
  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case dnp: DomainNamePartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }
}




/**
 * Usage: Aggregation [partitions] [numElem] [blockSize]
 */
object SparkPartitioner {

  def listFiles(iter: RemoteIterator[LocatedFileStatus]) = {
    def go(iter: RemoteIterator[LocatedFileStatus], acc: List[URI]): List[URI] = {
      if (iter.hasNext) {
        val uri = iter.next.getPath.toUri
        go(iter, uri :: acc)
      } else {
        acc
      }
    }
    go(iter, List.empty[java.net.URI])
  }



  def readfile(path: Option[String], spark: SparkSession): Option[DataFrame] = {
    //   if(path!=null)
    //      Some(spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path))
    //    else
    //      None
    path match {
      case Some(x) => Some(spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(x))
      case None    => None
    }

  }
  def mergDF(x: Option[DataFrame], y: Option[DataFrame]): Option[DataFrame] = {
    (x, y) match {
      case (Some(x), Some(y)) => Some(x.union(y))
      case (Some(x), None)    => Some(x)
      case (None, Some(y))    => Some(y)
      case (None, None)       => None
    }

  }
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Broadcast Test").master("local[2]")
      .getOrCreate()
    val df1 = readfile(Some("C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\2010-12-01.csv"), spark)
    val df2 = readfile(Some("C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\2010-12-02.csv"), spark)
    val df3 = readfile(None, spark)
    val df4 = readfile(Some("C:\\Users\\chenwang2017\\git\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\2010-12-03.csv"), spark)

    val merged = Seq(df1, df2, df3, df4).reduceLeft((x, y) => mergDF(x, y))
    val mergedDF=merged.get
    println("********************************"+ mergedDF.show(false))
    mergedDF.registerTempTable("temp_1");
    spark.sql("select CustomerID ,count(*) as ct from temp_1 group by  CustomerID order by ct desc ").show()
    val schema=mergedDF.schema
    val pairRDD=mergedDF.rdd.map(row=>(row(row.fieldIndex("CustomerID")),row))
    val customizedPairRDD=pairRDD.partitionBy(new DomainNamePartitioner(200))
    val customizedDF=spark.createDataFrame(customizedPairRDD.values,schema)
   println("*********************CUSTOMIZED***********")
   customizedDF.show(false)
     println("number of partitions:"+customizedDF.rdd.getNumPartitions)
     import spark.implicits._
     customizedDF.rdd.mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}.toDF("partition_number","number_of_records").show(200)
    println("df1=" + df1.map(_.count()) + " df2=" + df2.get.count + " df3=" + df3.map(_.count()) + " df4=" + df4.get.count + " meged=" + merged.get.count)

    spark.stop()
  }
}
// scalastyle:on println
