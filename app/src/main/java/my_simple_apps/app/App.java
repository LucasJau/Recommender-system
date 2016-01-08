package my_simple_apps.app;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Serializable;
import scala.Tuple2;

import javax.swing.*;

@SuppressWarnings("rawtypes")
class ComparatorForRating implements Comparator
{

	public int compare(Object o1, Object o2) {
		Rating r1 = (Rating)o1;
		Rating r2 = (Rating)o2;
		if(r1.rating() > r2.rating())
			return -1;
		else if(r1.rating() < r2.rating())
			return 1;
		else 
			return 0;
	}
	
}
class Compute implements Serializable
{
	public double computeRmse(MatrixFactorizationModel model, JavaRDD jr, long n)
	{
		JavaPairRDD<Integer, Integer> predicts = jr.mapToPair(new PairFunction<Rating, Integer, Integer>(){
			public Tuple2<Integer, Integer> call(Rating r)
			{
				return new Tuple2(r.user(), r.product());
			}
		});//出错行
		JavaPairRDD<Tuple2, Tuple2<Double, Double>> preRating = model.predict(predicts).mapToPair(new PairFunction<Rating, Tuple2, Double>() {

            public Tuple2<Tuple2, Double> call(Rating r) throws Exception {

				return new Tuple2(new Tuple2(r.user(),r.product()), r.rating());
			}
		}).join(jr.mapToPair(new PairFunction<Rating, Tuple2, Double>() {
            public Tuple2 call(Rating r) throws Exception {
                return new Tuple2(new Tuple2(r.user(), r.product()), r.rating());
            }
        }));
        JavaRDD<Tuple2<Double, Double>> pNr = preRating.values();
        double rmse = pNr.map(new Function<Tuple2<Double, Double>, Double>() {
            public Double call(Tuple2<Double, Double> t) throws Exception {
                return (t._1 - t._2) * (t._1 - t._2);
            }
        }).reduce(new Function2<Double, Double, Double>() {
            public Double call(Double num1, Double num2) throws Exception {
                return num1 + num2;
            }
        })/n;
		return rmse;
	}
}
public class App implements Serializable
{
    @SuppressWarnings("serial")
    public List<Tuple2<Integer, String>> getMovieName(JavaSparkContext sc)
    {
        JavaRDD<String> movieData = sc.textFile("file:///D:/ml-1m/movies.dat").cache();
        JavaPairRDD<Integer, String> movies = movieData.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String str[] = s.split("::");
                return new Tuple2<Integer, String>(Integer.parseInt(str[0]), str[1]);
            }
        });
        Random random = new Random(System.currentTimeMillis());
        final List<Integer> randNum = new ArrayList<Integer>();
        for(int i = 0; i < 20; i++)
        {
            randNum.add(random.nextInt(3952));
        }
        List<Tuple2<Integer, String>> movieNames = movies.filter(new Function<Tuple2<Integer, String>, Boolean>() {
            public Boolean call(Tuple2<Integer, String> t) throws Exception {
                if (randNum.contains(t._1))
                    return true;
                else
                    return false;
            }
        }).collect();
        return movieNames;
    }
    public List<String> run()
    {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String logFile = "file:///D:/ml-1m";
        JavaRDD<String> ratingData = sc.textFile(logFile + "/ratings.dat").cache();
        JavaRDD<String> movieData = sc.textFile(logFile + "/movies.dat").cache();
        JavaPairRDD<Long, Rating> ratings = ratingData.mapToPair(new PairFunction<String, Long, Rating>() {
            public Tuple2<Long, Rating> call(String s) {
                String str[] = s.split("::");
                return new Tuple2<Long, Rating>(Long.parseLong(str[3]) % 10,
                        new Rating(Integer.parseInt(str[0]), Integer.parseInt(str[1]), Double.parseDouble(str[2])));
            }
        });
        JavaPairRDD<Integer, String> movies = movieData.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String str[] = s.split("::");
                return new Tuple2<Integer, String>(Integer.parseInt(str[0]), str[1]);
            }
        });
        JavaRDD<Rating> training = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>() {
            public Boolean call(Tuple2<Long,Rating> t){return t._1 < 8;}
        }).values().cache();
        JavaRDD<Rating> testing = ratings.filter(new Function<Tuple2<Long, Rating>, Boolean>(){
            public Boolean call(Tuple2<Long,Rating> t){return t._1 >= 8;}
        }).values().cache();
        JavaRDD<Rating> myRating = sc.textFile(logFile + "/freshman.dat").map(new Function<String, Rating>() {
            public Rating call(String s) throws Exception {
                String str[] = s.split("::");
                return new Rating(Integer.parseInt(str[0]), Integer.parseInt(str[1]), Double.parseDouble(str[2]));
            }
        });
        long n = testing.count();
        MatrixFactorizationModel model = null;
        MatrixFactorizationModel bestModel = null;
        double bestRsme = Double.MAX_VALUE;
        int bestRank = 0;
        int bestNumlter = 0;
        double bestLambda = 0.0;
        for(int rank = 10; rank < 12; rank ++)
        {
            for(int numlter = 15; numlter < 18; numlter ++)
            {
                model = ALS.train(JavaRDD.toRDD(training.union(myRating)), rank, numlter, 0.01);
                Compute com = new Compute();
                double rsme = com.computeRmse(model, testing, n);
                if(bestRsme > rsme) {
                    bestRank = rank;
                    bestNumlter = numlter;
                    bestRsme = rsme;
                    bestModel = model;
                }
            }
        }
        System.out.println("best rank: " + bestRank + ", best numlter: " + bestNumlter + ", best lambda: " + bestLambda);
        final List<Integer> ratedMovies = myRating.map(new Function<Rating, Integer>(){
            public Integer call(Rating r)
            {
                return r.product();
            }
        }).collect();
        JavaRDD<Integer> candidates = sc.parallelize(movies.keys().filter(new Function<Integer, Boolean>(){
            public Boolean call(Integer i)
            {
                return !ratedMovies.contains(i);
            }
        }).collect());
        final List<Rating> preRating = bestModel.predict(candidates.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer i) {

                return new Tuple2<Integer, Integer>(0, i);
            }
        })).collect();
        Collections.sort(preRating, new ComparatorForRating());
        List recMovies = movies.filter(new Function<Tuple2<Integer, String>, Boolean>(){
            public Boolean call(Tuple2<Integer, String> t)
            {
                for(int i = 0; i < 10; i++)
                {
                    if(t._1 == preRating.get(i).product())
                        return true;
                }
                return false;
            }
        }).values().collect();
        System.out.println(bestNumlter +" "+ bestRank+" " + bestRsme);
        return recMovies;
    }
}
