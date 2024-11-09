package com.genesys.spark.tools;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * Batch Aggregation
 */
public class BatchAggregator {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(BatchAggregator.class);


    static void run(SparkSession spark, String path,String outputPath) throws IOException {
        logger.info("BatchAggregator starting");

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
        aggregratedDF.repartition(1).write().format("csv")
                .mode("overwrite")
                .option("header","true")
                .option("sep",",")
                .save(outputPath);
        logger.info("BatchAggregator finished");
    }

    public static void main(String[] args)
            throws  IOException {

        String path="/Users/chengwang/IdeaProjects/spark-tools/data/input/genesys/*.csv";
        String outputpath= String.format("/Users/chengwang/IdeaProjects/spark-tools/data/output/genesys/batch-agg-%d.csv",System.currentTimeMillis() );
        for(String arg:args)
            System.out.println("****** "+arg);
        if(args.length >=2){
            path=args[0];
            outputpath=args[1];
        }
        SparkSession spark = SparkSession.builder().appName("BatchAggregator").master("local[*]").getOrCreate();
        spark.sqlContext().udf().register("timestamp2bucket", new Timestamp2bucket(), DataTypes.StringType);
        run(spark, path,outputpath);
        spark.close();
    }
}
