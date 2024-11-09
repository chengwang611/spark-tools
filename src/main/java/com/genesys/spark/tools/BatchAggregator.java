package com.genesys.spark.tools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class BatchAggregator {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BatchAggregator.class);
    static void run(SparkSession spark, String path) throws IOException {


        Dataset csvDF = spark.read().format("csv").option("header", true).option("sep", ",").option("inferSchema", true)
                .load(path);
        csvDF.show();
        Dataset buckedDF= csvDF.withColumn("bucket",callUDF("timestamp2bucket", col("Timestamp")));
        buckedDF.printSchema();

        buckedDF.show(false);
        Dataset aggregratedDF=buckedDF.filter(col("bucket").isNotNull())
                .groupBy("Metric","bucket")
                .agg(avg("Value").alias("Average"));
        aggregratedDF.show(false);

    }

    public static void main(String[] args)
            throws  IOException {
        String path="/Users/chengwang/IdeaProjects/spark-tools/data/input/genesys/*.csv";
        SparkSession spark = SparkSession.builder().appName("BatchAggregator").master("local[*]").getOrCreate();
        spark.sqlContext().udf().register("timestamp2bucket", new Timestamp2bucket(), DataTypes.StringType);
        run(spark, path);
        spark.close();
    }
}
