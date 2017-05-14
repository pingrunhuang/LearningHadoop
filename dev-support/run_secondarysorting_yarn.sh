#!/bin/bash
echo "JAVA_HOME=$JAVA_HOME"
export APP_HOME=/Users/Frank/dev/LearningHadoop/SecondarySort
export HADOOP_HOME=/usr/local/hadoop-2.6.5
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/usr/local/spark-2.1.0-bin-hadoop2.6/
export APP_JAR=$APP_HOME/target/SecondarySort-1.0-SNAPSHOT.jar

# define input parameters
INPUT="$APP_HOME/src/main/resources/input_data/timeseries.txt"
OUTPUT="$APP_HOME/src/main/resources/output"


#    --executor-memory 2G \
#    --total-executor-cores 20 \
# submit the app to the yarn cluster
prog=spark.SecondarySort
$SPARK_HOME/bin/spark-submit  \
    --class $prog \
    --master yarn-cluster \
    $APP_JAR \
    $INPUT