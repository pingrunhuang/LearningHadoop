#!/bin/bash
echo "JAVA_HOME=$JAVA_HOME"
export APP_HOME=/Users/Frank/dev/LearningHadoop/TopN
export SPARK_HOME=/usr/local/spark-2.1.0-bin-hadoop2.6
export SPARK_MASTER=spark://localhost:7077
export APP_JAR=$APP_HOME/target/TopN-1.0-SNAPSHOT.jar
# define input parameters
INPUT="hdfs://127.0.0.1:9000/data/"
OUTPUT="hdfs://127.0.0.1:9000/output/1/"

#running on the spark standalone cluster
prog=Spark_Non_UniqueKey.TopNNonUnique
N=3
$SPARK_HOME/bin/spark-submit  \
    --class $prog \
    --master $SPARK_MASTER \
    --executor-memory 2G \
    --total-executor-cores 20 \
    --jars /Users/Frank/dev/LearningHadoop/Util/target/Util-1.0-SNAPSHOT.jar \
    $APP_JAR \
    $INPUT \
    $OUTPUT \
    $N
