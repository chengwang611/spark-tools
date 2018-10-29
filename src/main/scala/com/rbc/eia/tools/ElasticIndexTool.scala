package com.rbc.eia.tools


import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.collection.mutable.ListBuffer
import scala.beans.BeanProperty
import java.io.{File, FileInputStream}


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.elasticsearch.spark._
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import scala.collection.JavaConversions._
import com.rbc.eia.tools.entities.ConfigProp

object ElasticIndexTool {
  
  
 def loadConfig(path:String): ConfigProp= {
     val filename = path
     val input = new FileInputStream(new File(filename))
     val yaml = new Yaml(new Constructor(classOf[ConfigProp]))
     val configProp = yaml.load(input).asInstanceOf[ConfigProp]
     configProp
 }
 def fileType(path:String):Int ={
   if ( path.endsWith("avro")) 1
   else if (path.endsWith("parquet")) 2 
   else -1
 }
  
 def addColumn(df:Dataset[Row],sourceColName:String,targetColName:String,typee:String):Dataset[Row] ={
   
   
   if("toDouble".equalsIgnoreCase(typee)){
      val toDouble = udf[Double, String]( _.toDouble)
      return  df.withColumn(targetColName,  toDouble(df(sourceColName)))
    }
   return null
   
 }
 
 def doColumnConvertion(df:Dataset[Row],conversions:List[String]):Dataset[Row] ={
   var dftemp=df
   for(c<-conversions){
     val conversion=c.split("&")
      dftemp=addColumn(dftemp,conversion(0),conversion(1),conversion(2))
   }
   
   return dftemp
 }
 def main(args: Array[String]){

    val config=loadConfig(args(0))
    println(config.getDataPath)
    // initialize spark context
    val conf = new SparkConf().setAppName(ElasticIndexTool.getClass.getName)
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.sqlContext.implicits._

    // Elastic connection parameters
    val elasticConf: Map[String, String] = Map("es.nodes" -> config.esNodes,
      "es.clustername" -> config.esClustername,"es.mapping.id" -> config.esMappingId,
      "es.index.auto.create" -> config.esIndexAutoCreate)

    val indexName = config.getIndexName
    val mappingName = config.mappingName
    // Dummy DataFrame
    val df  = fileType(config.getDataPath) match {
        case 1 => spark.read.format("com.databricks.spark.avro").load(config.getDataPath)
        case 2 => spark.read.parquet(config.getDataPath)
   }
    

////    //demo data type conversion-hard coded for test purpose
//    val toDouble = udf[Double, String]( _.toDouble)
//    
//    val columnsToConvert=config.columnsToConvert.toList
//    val df2 =doColumnConvertion(df,columnsToConvert)
//    val concat: (Column, Column) => Column = (x, y) => {concat_ws(",",x,y) }
//  //  val dff=df2.withColumn("indexId",concat($"visitStartTime",$"visitStartTime")).withColumn("location",concat($"latitudeDouble",$"longitudeDouble")).select("indexId","location")
//    val dff=df2.withColumn("indexId",concat($"fullVisitorId",$"visitStartTime")).withColumn("location",concat($"latitudeDouble",$"longitudeDouble")).
//   withColumn("visitStartDate", $"visitStartTime".cast("timestamp").cast(TimestampType)).select("indexId","geoNetwork","hits.customDimensions","hits.time","hits.appInfo","device","visitStartDate","location").sample(true, 0.1)
//             .select("indexId","hits.customDimensions","visitStartDate","location")
//    
//		df.printSchema()
//		df.show(false)

    // Write elasticsearch
		if("true".equalsIgnoreCase(config.doIndex))
      df.saveToEs(s"${indexName}/${mappingName}", elasticConf)

    // terminate spark context
    spark.stop()
  }
}