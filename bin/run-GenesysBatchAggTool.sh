#!/bin/bash
#JAVA_HOME=/opt/java/jre1.8.0_112/
#Spark Queue specification
JAR_PATH=$1 ## /Users/chengwang/IdeaProjects/spark-tools/target/batch-aggregation-tool-1.0.0.jar
PATH_TO_INPUT=$2 ##$PROJECT_BASE/data/input/genesys
PATH_TO_OUTPUT=$3 ##$PROJECT_BASE/data/output/genesys
echo "jar file path: $JAR_PATH"
echo "input path: $PATH_TO_INPUT"
echo "output path: $PATH_TO_OUTPUT"
java -cp  $JAR_PATH com.genesys.spark.tools.BatchAggregator \
$PATH_TO_INPUT  $PATH_TO_OUTPUT\
