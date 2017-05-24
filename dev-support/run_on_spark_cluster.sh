#!/usr/bin/env bash
export JAVA_HOME=/usr/java/jdk7
# java is defined at $JAVA_HOME/bin/java
export BOOK_HOME=/home/mp/data-algorithms-book
export SPARK_HOME=/usr/local/spark-1.6.0
export SPARK_MASTER=spark://myserver100:7077
export SPARK_JAR=$BOOK_HOME/lib/spark-assembly-1.6.0-hadoop2.6.0.jar
export APP_JAR=$BOOK_HOME/dist/data_algorithms_book.jar
#
# build all other dependent jars in OTHER_JARS
JARS=`find $BOOK_HOME/lib -name '*.jar'`
OTHER_JARS=""
for J in $JARS ; do
   OTHER_JARS=$J,$OTHER_JARS
done
#
P1=<input-parameter-1-for-DRIVER_CLASS_NAME>
P2=<input-parameter-2-for-DRIVER_CLASS_NAME>
DRIVER_CLASS_NAME=<your-driver-class-name>
$SPARK_HOME/bin/spark-submit --class $DRIVER_CLASS_NAME \
    --master $SPARK_MASTER \
    --num-executors 12 \
    --driver-memory 3g \
    --executor-memory 7g \
    --executor-cores 12 \
    --jars $OTHER_JARS \
    $APP_JAR $P1 $P2