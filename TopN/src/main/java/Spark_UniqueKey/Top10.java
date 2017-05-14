package Spark_UniqueKey;


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

public class Top10 {
    private static Logger logger = LoggerFactory.getLogger(Top10.class);
    private static int N = 10;
    public static void main(String[] args){
        if (args.length < 1){
            logger.error("Usage: Top10 <hdfs file dir>");
            System.exit(1);
        }
        String INPUT_DATA = args[0];
        logger.info("inputPath: <hdfs-file>=" + INPUT_DATA);

        JavaSparkContext ctx = new JavaSparkContext();

        // we can specify if we want the top n or bottom n
        String direction = "top";
        final Broadcast<String> broadcastDirection = ctx.broadcast(direction);

        // read a file from hdfs and convert to RDD
        JavaRDD<String> lines = ctx.textFile(INPUT_DATA,1);

        // PairFunction<T, K, V>
        // T -> Tuple2<K,V>
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(
                new PairFunction<
                        String, // input
                        String, // K
                        Integer>() { // V
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(
                                tokens[0], Integer.parseInt(tokens[1])
                        );
                    }
                });

        // use mapPartitions() to create a local top 10 list from different partitions
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(
                new FlatMapFunction<
                        Iterator<Tuple2<String, Integer>>,
                        SortedMap<Integer, String>
                        >() {
                    public Iterable<SortedMap<Integer, String>>
                    call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {

                        // same functionality as mapreduce's setup method
                        SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
                        // simulate the map() method
                        while(tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> tuple2 = tuple2Iterator.next();

                            top10.put(tuple2._2,tuple2._1);

                            if (top10.size() > N){
                                if (broadcastDirection.getValue().equals("top")){
                                    // delete the least frequency element
                                    top10.remove(top10.firstKey());
                                }else{
                                    // delete the bottom 10 element
                                    top10.remove(top10.lastKey());
                                }
                            }
                        }
                        // equal to cleanup()
                        return Collections.singletonList(top10);
                    }
                }
        );

        // use collect to get the final top 10
        SortedMap<Integer, String> finalTop10 = new TreeMap<Integer, String>();
        List<SortedMap<Integer, String>> allTop10 = partitions.collect();
        for (SortedMap<Integer, String> localTop10 : allTop10){
            // notice how to retrieve the entry in the entryset
            for (Map.Entry<Integer, String> entry : localTop10.entrySet()){
                finalTop10.put(entry.getKey(), entry.getValue());
                if (finalTop10.size() > N){
                    if (broadcastDirection.getValue().equals("top")){
                        // delete the least frequency element
                        finalTop10.remove(finalTop10.firstKey());
                    }else{
                        // delete the bottom 10 element
                        finalTop10.remove(finalTop10.lastKey());
                    }
                }
            }
        }

        // another way to get the final top 10 list by using JavaRDD.reduce()
        SortedMap<Integer, String> finalTop10_2 = partitions.reduce(
                new Function2<
                        SortedMap<Integer, String>, // input 1
                        SortedMap<Integer, String>, // input 2
                        SortedMap<Integer, String>>() { // result
                    public SortedMap<Integer, String> call(SortedMap<Integer, String> v1,
                                                           SortedMap<Integer, String> v2) throws Exception {
                        SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();

                        for (Map.Entry<Integer, String> m1 : v1.entrySet()){
                            top10.put(m1.getKey(), m1.getValue());
                            if (top10.size() > N){
                                if (broadcastDirection.getValue().equals("top")){
                                    // delete the least frequency element
                                    top10.remove(top10.firstKey());
                                }else{
                                    // delete the bottom 10 element
                                    top10.remove(top10.lastKey());
                                }
                            }
                        }

                        for (Map.Entry<Integer, String> m2 : v2.entrySet()){
                            top10.put(m2.getKey(), m2.getValue());
                            if (top10.size() > N){
                                if (broadcastDirection.getValue().equals("top")){
                                    // delete the least frequency element
                                    top10.remove(top10.firstKey());
                                }else{
                                    // delete the bottom 10 element
                                    top10.remove(top10.lastKey());
                                }
                            }
                        }

                        return top10;
                    }
                }
        );

        logger.info("===top-10 list===");
        for (Map.Entry<Integer, String> entry : finalTop10.entrySet()){
            logger.info(entry.getKey() + "--" + entry.getValue());
        }

    }
}
