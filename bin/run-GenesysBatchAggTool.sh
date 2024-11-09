#!/bin/bash
#JAVA_HOME=/opt/java/jre1.8.0_112/
#Spark Queue specification
PROJECT_BASE=/Users/chengwang/IdeaProjects/spark-tools
PATH_TO_INPUT=$PROJECT_BASE/data/input/genesys

java -cp  $PROJECT_BASE/target/batch-aggregation-tool-1.0.0.jar com.genesys.spark.tools.BatchAggregator $PATH_TO_INPUT\
