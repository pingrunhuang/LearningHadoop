package Spark_Non_UniqueKey;

import Util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Top10UsingTakeOrdered implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(Top10UsingTakeOrdered.class);
    static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable{
        final static MyTupleComparator INSTANCE = new MyTupleComparator();

        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o1._2.compareTo(o2._2); // return top N descending
//            return -o1._2.compareTo(o2._2); // return bottom N ascending
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            logger.error("Usage: TopNNonUnique <input-dir> <output-path> <topN>");
            System.exit(1);
        }
        String inputDir = args[0];
        int topN = Integer.parseInt(args[1]);

        JavaSparkContext context = SparkUtil.createJavaSparkContext("Top_10_using_TakeOrdered()");

        JavaPairRDD<String, String> lines = context.wholeTextFiles(inputDir, 1);
        lines.values().saveAsTextFile("hdfs://output/1");

        // RDD partition
        JavaRDD<String> rdd = lines.values().coalesce(9);

        // T to (K,V)
        JavaPairRDD<String, Integer> kv = rdd.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
                    }
                }
        );
        kv.saveAsTextFile("hdfs//output/2");

        // get the uniqueKey
        JavaPairRDD<String, Integer> uniqueKey = kv.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        // instead of assign the RDD to the partition again, here we just use the takeordered method
        // call the takeordered()
        List<Tuple2<String, Integer>> topNResult = uniqueKey.takeOrdered(topN, MyTupleComparator.INSTANCE);

        for (Tuple2<String, Integer> entry : topNResult) {
            logger.info(entry._1 + "--" + entry._2);
        }

        System.exit(1);
    }

}
