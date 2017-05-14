package Spark_UniqueKey;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/*
* use Broadcast object to specify the value of N
* */
public class TopN {
    private int topN = 100;
    JavaSparkContext ctx = new JavaSparkContext();
    final Broadcast<Integer> broadcastTopN = ctx.broadcast(topN);

    // retrieve the value
    final int N = broadcastTopN.getValue();

    // map

    // reduce

}
