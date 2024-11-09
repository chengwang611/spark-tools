# GENESYS-BATCH-AGGREGATION-Tool
the base project for batch aggregation spark application at **SPARK2.2.0 AND JAVA 1.8**

Assume the project root is :
PROJECT_ROOT=/Users/chengwang/IdeaProjects/spark-tools

* to build need java 1.8 ,maven 3.8+ installed in a terminal :
1. cd $PROJECT_ROOT
2. export JAVA_HOME=PATH_TO_JAVA_1.8_HOME
3.  mvn clean package
    <br>batch-aggregation-tool-1.0.0.jar will be generated

* to run the java app.

1. ./bin/run-GenesysBatchAggTool.sh JAR_PATH PATH_TO_INPUT PATH_TO_OUTPUT
   <br>where <br> JAR_PATH is the absolute path to the jar file
   <br>      PATH_TO_INPUT is the absolute path to the input file directory
   <br>      PATH_TO_OUTPUT is the absolute path to the output file directory
<br> After finish,the output will be generated at  PATH_TO_OUTPUT:
for a reference of running terminal please refer: **build-run.log**