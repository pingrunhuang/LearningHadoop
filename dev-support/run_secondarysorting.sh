#!/bin/bash
echo "JAVA_HOME=$JAVA_HOME"
export APP_HOME=/Users/Frank/dev/LearningHadoop/SecondarySort
export SPARK_HOME=/usr/local/spark-2.1.0-bin-hadoop2.6
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$APP_HOME/target/SecondarySort-1.0-SNAPSHOT.jar
# define input parameters
INPUT="$APP_HOME/src/main/resources/input_data/timeseries.txt"
OUTPUT="$APP_HOME/src/main/resources/output"

#running on the spark standalone cluster
prog=spark.SecondarySort
$SPARK_HOME/bin/spark-submit  \
    --class $prog \
    --master $SPARK_MASTER \
    --executor-memory 2G \
    --total-executor-cores 20 \
    $APP_JAR \
    $INPUT \
    $OUTPUT