#!/bin/bash
JAVA_HOME=/opt/java/jre1.8.0_112/
#Spark Queue specification
SPARK_QUEUE_ID=DUQ20
SPARK_CORE_NUM=8

spark-submit  --queue $SPARK_QUEUE_ID   \
	      --class com.rbc.eia.tools.ElasticIndexTool --master yarn-client \
	      --driver-memory 8g  \
              --num-executors 8  --executor-memory 4G   --total-executor-cores 8  elasticsearchtool-1.0.0.jar \
              PATH_TO_CONFIG_FILE