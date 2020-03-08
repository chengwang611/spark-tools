/*

 */

// scalastyle:off println
package com.paytmlabs.weblog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ to_date, to_timestamp }
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
/**
 * Usage: Aggregation [partitions] [numElem] [blockSize]
 */

case class Event(EvenType: String, Date: String, Price: Double)
object SparkBSVEvent {
    def parseEvent(s: String): Option[Event] = {
    try {
      val event = s.trim().split(',').toSeq
      Some(Event(event(0), event(1), event(2).toDouble))
    } catch {
      case e: Exception => None
    }
  }
 
  def toEvent(s: String): Seq[Option[Event]] = {
    val eventStrs = s.split('|').toSeq
    val events = for (e <- eventStrs) yield {
      parseEvent(e)
    }
    events
  }


  def main(args: Array[String]) {

    val sparkConf = new SparkConf
    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")
    val spark = SparkSession.builder.appName("bsv-event").config(sparkConf)
      .getOrCreate

    val path = "C://Users//chenwang2017//git//DNA-Elasticsearch-Indexing-Tool//DNA-Elasticsearch-Indexing-Tool//data//trades.bsv"
    val rdd = spark.sparkContext.textFile(path)
    val rssEventStrRDD = rdd.flatMap(toEvent)
    val rssEventRDD = rssEventStrRDD.filter(!_.isEmpty).map(_.get)
    rssEventRDD.collect.foreach(println)
    val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val eventDF = rssEventRDD.toDF()
      .withColumn("Date", to_timestamp($"Date", "ddMMyyyy:HH:mm"))
    eventDF.show(false)
    eventDF.registerTempTable("eventTable")
    spark.sql("select * from eventTable order by Date").show
    //add the sql  for the questions
    spark.stop()
  }
}
// scalastyle:on println
