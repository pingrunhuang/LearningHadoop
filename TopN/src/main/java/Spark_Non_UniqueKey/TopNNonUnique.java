package Spark_Non_UniqueKey;

import Util.SparkUtil;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class TopNNonUnique {
    private static Logger logger = LoggerFactory.getLogger(TopNNonUnique.class);
    public static void main(String[] args) throws Exception {
        if (args.length < 3){
            logger.error("Usage: TopNNonUnique <input-path> <output-path> <topN>");
            System.exit(1);
        }
        final String inputDir = args[0];
        final String outputPath = args[1];
        final int N = Integer.parseInt(args[2]);

        JavaSparkContext context = SparkUtil.createJavaSparkContext("TopNNonUnique");

        // broadcast the value N to all the nodes
        final Broadcast<Integer> topN = context.broadcast(N);

        // create our first RDD
        // read the whole directory
        JavaPairRDD<String, String> lines = context.wholeTextFiles(inputDir, 1);
        //key is the path of each file and value is the content of each file
        lines.values().saveAsTextFile(outputPath);

        // RDD partition
        // num_partition depends on how many cores or executor
        // generally speaking: num_partitions = 2 * num_executors * cores_per_executor
        JavaPairRDD<String, String> rdd = lines.coalesce(9);
        rdd.keys().saveAsTextFile("hdfs://data/output/rdd_partition_result_key");
        rdd.values().saveAsTextFile("hdfs://data/output/rdd_partition_result_value");


        // generate (K,V) pair with the input T
        // K -> URL, V -> count
        JavaPairRDD<String, Integer> kv = rdd.values().mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[2]));
                    }
                }
        );
        kv.saveAsTextFile("hdfs://data/output/2");

        // reduce the duplicated key
        JavaPairRDD<String, Integer> uniqueKeys =
                kv.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });
        uniqueKeys.saveAsTextFile("hdfs://data/output/unique_key_result");

        // create top10 list for each partition
        JavaRDD<SortedMap<Integer, String>> partitions = uniqueKeys.mapPartitions(
                new FlatMapFunction<
                        Iterator<Tuple2<String, Integer>>,
                        SortedMap<Integer, String>>() {
                    public Iterable<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        // notice we should use this way to retrive the broadcasted value for each partition
                        final int N = topN.value();
                        SortedMap<Integer, String> localTopN = new TreeMap<Integer, String>();
                        while (tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> entry = tuple2Iterator.next();
                            localTopN.put(entry._2, entry._1);
                            if(localTopN.size() > N){
                                localTopN.remove(localTopN.firstKey());
                            }
                        }
                        return Collections.singletonList(localTopN);
                    }
                }
        );
        partitions.saveAsTextFile("hdfs://data/output/local_topN");

        // final topN
        SortedMap<Integer, String> finalTopN = new TreeMap<Integer, String>();
        // the collect method will return each partition's result as a list
        List<SortedMap<Integer, String>> allTopN = partitions.collect();
        for (SortedMap<Integer, String> localTopN : allTopN){
            for (Map.Entry<Integer, String> entry : localTopN.entrySet()){
                finalTopN.put(entry.getKey(), entry.getValue());
                if (finalTopN.size() > N){
                    finalTopN.remove(finalTopN.firstKey());
                }
            }
        }

        // output result
        for (Map.Entry<Integer, String> entry : finalTopN.entrySet()){
            logger.info("key=" + entry.getValue() + " value=" + entry.getKey());
        }


        System.exit(1);
    }
}
