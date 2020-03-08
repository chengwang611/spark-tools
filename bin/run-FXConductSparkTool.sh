#!/bin/bash
JAVA_HOME=/opt/java/jre1.8.0_112/
#Spark Queue specification
SPARK_QUEUE_ID=DUQ20
SPARK_CORE_NUM=8
export SPARK_MAJOR_VERSION=2

spark-submit    \
	      --class com.spark.tool.Aggregation --master yarn-client \
	      --driver-memory 4g  \
              --num-executors 2  --executor-memory 4G   --total-executor-cores 4  /home/spark/fxconduct/target/elasticsearchtool-1.0.0.jar \
              path1=/tmp/fxconduct/2019-07-28.csv path2=/tmp/fxconduct/2019-07-28.csv keyCols=InvoiceNo,StockCode flatColumn=Description outputPath=/tmp/fxconduct/output/