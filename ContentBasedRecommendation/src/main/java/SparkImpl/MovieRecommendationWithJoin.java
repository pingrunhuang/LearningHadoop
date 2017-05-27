package SparkImpl;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.List;

public class MovieRecommendationWithJoin {
    private static final Logger logger = LoggerFactory.getLogger(MovieRecommendationWithJoin.class);
    public static void main(String[] args){
        if (args.length < 1){
            logger.error("Usage: MovieRecommendationWithJoin <users-ratings>");
            System.exit(1);
        }
        String inputFile = args[0];
        logger.info("input data file=" + inputFile);

        // 1. create spark context
        JavaSparkContext javaSparkContext = new JavaSparkContext();

        // 2. read file from hdfs
        JavaRDD<String> userRatings = javaSparkContext.textFile(inputFile);

        // 3. find the number of ratings for each movie
        // how to implement a mapper by spark: (user, movie, rating) -> (movie, user, rating)
        JavaPairRDD<String, Tuple2<String, Integer>> moviesRDD = userRatings.mapToPair(
                new PairFunction<
                        String, // from: <user>, <movie>, <rating>
                        String, // to: <movie>
                        Tuple2<String, Integer> // Tuple2<user, rating>
                        >() {
                    public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                        String[] record = s.split("\t");
                        String user = record[0];
                        String movie = record[1];
                        Integer rating = Integer.parseInt(record[2]);
                        Tuple2<String, Integer> value = new Tuple2<String, Integer>(user, rating);
                        return new Tuple2<String, Tuple2<String, Integer>>(movie, value);
                    }
                }
        );
        // use collect() to test the result
        // collect method can take a lot of resources, so it is not recommended to use it in the production environment
        logger.debug("===debug1:moviesRDD:K = <movie>, V = Tuple2<user,rating>===");
        List<Tuple2<String, Tuple2<String, Integer>>> debug1 = moviesRDD.collect();
        for (Tuple2<String, Tuple2<String, Integer>> entry : debug1){
            logger.debug("debug1 key="+entry._1+"\t value="+entry._2);
        }

        // 4. separate into groups by movie
        // (movie, user, rating) -> (movie, List<Tuple2<user, rating>>)
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> moviesGrouped = moviesRDD.groupByKey();
        logger.debug("=== debug2: moviesGrouped: K=<movie>, V=Iterable<Tuple2<String, Integer>> ===");
        List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> debug2 = moviesGrouped.collect();
        for (Tuple2<String, Iterable<Tuple2<String, Integer>>> entry : debug2){
            logger.debug("debug2 key="+entry._1+"\t value="+entry._2);
        }

        // find the number of ratings for each movie by the each user
        // equal to reducer
        // (movie, List<Tuple2<user, rating>>) -> (user, Tuple3<movie, rating, numberOfRaters>)
        JavaPairRDD<String, Tuple3<String, Integer, Integer>> usersRDD =
                moviesGrouped.flatMapToPair(
                        new PairFlatMapFunction<
                                Tuple2<String, Iterable<Tuple2<String, Integer>>>, //input record:(movie, [....])
                                String, // output-key:user
                                Tuple3<String, Integer, Integer> //output-value:(movie, rating, number)
                                >() {
                            public Iterable<Tuple2<String, Tuple3<String, Integer, Integer>>>
                            call(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) throws Exception {

                                String movie = stringIterableTuple2._1;
                                Iterable<Tuple2<String, Integer>> pairsOfUsersAndRatings = stringIterableTuple2._2;
                                int numberOfRatings = 0;
                                for (Tuple2<String,Integer> t2 : pairsOfUsersAndRatings){
                                    numberOfRatings++;
                                }

                                // emit the key value
                                List<Tuple2<String, Tuple3<String, Integer, Integer>>> result =
                                        new ArrayList<Tuple2<String, Tuple3<String, Integer, Integer>>>();
                                for (Tuple2<String,Integer> t2 : pairsOfUsersAndRatings){
                                    String user = t2._1;
                                    Integer rating = t2._2;
                                    Tuple3<String, Integer, Integer> value =
                                            new Tuple3<String, Integer, Integer>(movie,rating,numberOfRatings);
                                    result.add(
                                            new Tuple2<String, Tuple3<String, Integer, Integer>>(user, value)
                                    );
                                }
                                return result;
                            }
                        }
                );
        // test
        logger.debug("=== debug3: moviesGrouped: K = user, V = Tuple3<movie, rating, number> ===");
        List<Tuple2<String, Tuple3<String, Integer, Integer>>> debug3 =
                usersRDD.collect();
        for (Tuple2<String, Tuple3<String, Integer, Integer>> t2:debug3){
            logger.debug("debug3 Key="+t2._1, "\t value=" + t2._2);
        }

        // 5. self join: join 2 same table
        // (user, (movie, rating, number)) -> (user, [(movie, rating, number),(movie, rating, number)])
        JavaPairRDD<
                String,
                Tuple2<
                        Tuple3<String, Integer, Integer>,
                        Tuple3<String, Integer, Integer>
                        >
                >
                joinedRDD = usersRDD.join(usersRDD);

        //test
        logger.debug("=== debug4: moviesGrouped: K = user, V = Tuple3<movie, rating, number> ===");
        List<Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>>
                debug4 = joinedRDD.collect();
        for (Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> entry : debug4){
            logger.debug("debug4 Key="+entry._1 + "\t Value="+entry._2);
        }


        // 6. the join function is applying join to every other record with the same key
        // therefore, there could be redundant records after joining
        // we should use filter function
        // (movie1, movie2) and (movie2, movie1) are the same
        JavaPairRDD<
                String,
                Tuple2<
                        Tuple3<String, Integer, Integer>,
                        Tuple3<String, Integer, Integer>>
                > filteredRDD = joinedRDD.filter(
                new Function<
                        Tuple2<
                                String,
                                Tuple2<
                                        Tuple3<String, Integer, Integer>,
                                        Tuple3<String, Integer, Integer>
                                        >
                                >, Boolean>() {
                    public Boolean call(Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>> v1) throws Exception {

                        Tuple3<String, Integer, Integer> movie1 = v1._2._1;
                        Tuple3<String, Integer, Integer> movie2 = v1._2._2;
                        String movieName1 = movie1._1();
                        String movieName2 = movie2._1();
                        if (movieName1.compareTo(movieName2) < 0){
                            // only when movie1 < movie2 will keep (true means keep)
                            return true;
                        }else{
                            // equals or larger, do filter (false means not keep)
                            return false;
                        }

                    }
                }
        );
        // test


        // 7.generate the (movie1, movie2) combination
        // result of this step will be used to calculate the coherence value: pearson, cosine, jaccard
        JavaPairRDD<Tuple2<String, String>,
                Tuple7<
                        Integer, // movie1.rating
                        Integer, // movie1.numOfRaters
                        Integer, // movie2.rating
                        Integer, // movie2.numOfRaters
                        Integer, // ratingProduct: movie1.rating * movie2.rating
                        Integer, // movie1.rating * movie1.rating
                        Integer> // movie2.rating * movie2.rating
                > moviePairs = filteredRDD.mapToPair(
                new PairFunction<
                        Tuple2<String, Tuple2<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>>, // input
                        Tuple2<String, String>, // output key
                        Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> // output value
                        >() {
                    public Tuple2<
                            Tuple2<String, String>,
                            Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
                            > call(
                                    Tuple2<String,
                                            Tuple2<
                                                    Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>
                                            > s) throws Exception {
                        Tuple3<String, Integer, Integer> movie1 = s._2()._1;
                        Tuple3<String, Integer, Integer> movie2 = s._2()._2;

                        Integer movie1_rating = movie1._2();
                        Integer movie2_rating = movie2._2();

                        Integer movie1_num = movie1._3();
                        Integer movie2_num = movie1._3();

                        Integer productRating = movie1_rating * movie2_rating;
                        Integer rating1Square = movie1_rating * movie1_rating;
                        Integer rating2Square = movie2_rating * movie2_rating;

                        Tuple2<String, String> return_key = new Tuple2<String, String>(movie1._1(), movie2._1());
                        Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> return_value =
                                new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                                        movie1_rating, movie1_num, movie1_rating, movie2_num, productRating, rating1Square, rating2Square
                                );
                        return new Tuple2<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>(
                                return_key, return_value
                        );
                    }
                }
        );


        // 8. group by the movie pair
        JavaPairRDD<
                Tuple2<String, String>,
                Iterable<Tuple7<
                        Integer, // movie1.rating
                        Integer, // movie1.numOfRaters
                        Integer, // movie2.rating
                        Integer, // movie2.numOfRaters
                        Integer, // ratingProduct: movie1.rating * movie2.rating
                        Integer, // movie1.rating * movie1.rating
                        Integer> // movie2.rating * movie2.rating
                        >
                > corrRDD = moviePairs.groupByKey();

        // 9. calculate the correlation value
        JavaPairRDD<Tuple2<String, String>, Tuple3<Double, Double, Double>> corr =
                corrRDD.mapValues(
                        new Function<
                                Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>, //input
                                Tuple3<Double, Double, Double> // output
                                >() {
                            public Tuple3<Double, Double, Double> call(Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> v1) throws Exception {
                                return calculateCorrelation(v1);
                            }
                        }
                );
    }

    private static Tuple3<Double, Double, Double> calculateCorrelation(
            Iterable<Tuple7<
                    Integer, // movie1.rating
                    Integer, // movie1.numOfRaters
                    Integer, // movie2.rating
                    Integer, // movie2.numOfRaters
                    Integer, // ratingProduct: movie1.rating * movie2.rating
                    Integer, // movie1.rating * movie1.rating
                    Integer> // movie2.rating * movie2.rating
                    > values
            ) {
        int groupSize = 0;          // the vector size on different direction
        int dotProduct = 0;         // rating product's sum
        int rating1Sum = 0;         // movie1's rating summary
        int rating2Sum = 0;         // movie2's rating summary
        int rating1NormSq = 0;      // rating1Squared's sum
        int rating2NormSq = 0;      // rating2Squared's sum
        int maxNum1 = 0;            // max of num1
        int maxNum2 = 0;            // max of num2

        for (Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer> value : values){
            groupSize++;
            rating1Sum += value._1();
            rating2Sum += value._3();
            rating1NormSq += value._6();
            rating2NormSq += value._7();
            dotProduct += value._5();
            maxNum1 = value._2() > maxNum1?value._2():maxNum1;
            maxNum2 = value._4() > maxNum2?value._4():maxNum2;
        }
        double pearson = calculatePearsonCorrelation(
                groupSize,
                dotProduct,
                rating1Sum,
                rating2Sum,
                rating1NormSq,
                rating2NormSq);

        double cosine = calculateCosineCorrelation(dotProduct,
                Math.sqrt(rating1NormSq),
                Math.sqrt(rating2NormSq));

        double jaccard = calculateJaccardCorrelation(groupSize, maxNum1, maxNum2);

        return  new Tuple3<Double,Double,Double>(pearson, cosine, jaccard);
    }


    private static double calculatePearsonCorrelation(
            double size,
            double dotProduct,
            double rating1Sum,
            double rating2Sum,
            double rating1NormSq,
            double rating2NormSq)  {

        double numerator = size * dotProduct - rating1Sum * rating2Sum;
        double denominator =
                Math.sqrt(size * rating1NormSq - rating1Sum * rating1Sum) *
                        Math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum);
        return numerator / denominator;
    }

    /**
     * The cosine similarity between two vectors A, B is
     *   dotProduct(A, B) / (norm(A) * norm(B))
     */
    private static double calculateCosineCorrelation(double dotProduct,
                                             double rating1Norm,
                                             double rating2Norm) {
        return dotProduct / (rating1Norm * rating2Norm);
    }

    /**
     * The Jaccard Similarity between two sets A, B is
     *   |Intersection(A, B)| / |Union(A, B)|
     */
    private static double calculateJaccardCorrelation(double inCommon,
                                              double totalA,
                                              double totalB) {
        double union = totalA + totalB - inCommon;
        return inCommon / union;
    }

}
