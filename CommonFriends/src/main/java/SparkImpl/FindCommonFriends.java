package SparkImpl;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;

public class FindCommonFriends {
    private static final Logger logger = LoggerFactory.getLogger(FindCommonFriends.class);
    public static void main(String[] args) throws Exception{
        if (args.length < 1){
            logger.error("Usage: FindCommonFriends <data-source>");
            System.exit(1);
        }
        logger.info("HDFS input file = " + args[0]);

        // create a javasparkcontext as a factory for creating RDD
        JavaSparkContext context = new JavaSparkContext();

        // Read file from hdfs
        JavaRDD<String> records = context.textFile(args[0],1);
        List<String> debug0 = records.collect();
        for (String data : debug0){
            logger.info("debug0 record = " + data);
        }

        /*
        * map the RDD from Tuple2(P, Fi) to key:Tuple2(P, Fi) value:[F1,F2,F3...Fn]
        * */

        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> pairs = records.flatMapToPair(
                new PairFlatMapFunction<
                        String,                 // input line
                        Tuple2<Long, Long>,     // K
                        Iterable<Long>          // V
                        >() {
                    public Iterable<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> call(String s) throws Exception {

                        String[] tokens = s.split(",");
                        long person = Long.parseLong(tokens[0]);

                        return null;

                    }
                }
        );
    }
}
