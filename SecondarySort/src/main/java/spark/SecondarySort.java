package spark;

/* This solution with spark used in memory to implement */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class SecondarySort {
    private static Logger logger = LoggerFactory.getLogger(SecondarySort.class);

    public static void main(String[] args){
        // 1. input and varify
        if(args.length < 1){
            logger.error("Usage: SecondarySort<file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);

        // connect to spark master by creating a JavaSparkContext
        final JavaSparkContext ctx = new JavaSparkContext();

        // create JavaRDD<String>
        JavaRDD<String> lines = ctx.textFile(inputPath,1);
        // create a reflector for matching key and value
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs =
                lines.mapToPair(new PairFunction<
                                        String,
                                        String,
                                        Tuple2<Integer,Integer>>(){
                    public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        logger.info(tokens[0] + ", " + tokens[1] + ", " + tokens[2]);
                        Integer time = new Integer(tokens[1]);
                        Integer value = new Integer(tokens[2]);
                        Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>
                                (time, value);
                        return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
                    }
                });

        // verifying the result using JavaRDD.collect() (not be used in real production since it will impact the efficiency)
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2 t : output) {
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            logger.info(t._1 + "," + timevalue._1 + "," + timevalue._1);
        }

        // grouping JavaPairRDD by key (name)
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups =
                pairs.groupByKey();

        // verify the last step
        logger.info("===DEBUG 1===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 =
                groups.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output2){
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            logger.info(t._1);
            for (Tuple2<Integer, Integer> t2 : list){
                logger.info(t2._1 + "," + t2._2);
            }
        }

        // start sorting result
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(
                (v1) -> {
                        List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>
                                ((Collection<? extends Tuple2<Integer, Integer>>) v1);
                        Collections.sort(newList, new TupleComparator());
                        return newList;
                    }
        );

        // print out the result
        logger.info("===DEBUG 2===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 =
                sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output3){
            Iterable<Tuple2<Integer, Integer>> tempList = t._2;
            logger.info("t._2 = " + t._2);
            for (Tuple2<Integer, Integer> t2 : tempList){
                logger.info(t2._1 + ", " + t2._2);
            }
        }

        // this is how to save the result
        sorted.saveAsTextFile(args[1]);
        logger.info("====");
    }
}
