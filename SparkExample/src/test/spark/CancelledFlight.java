package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class CancelledFlight {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airlines = sc.textFile("hdfs://localhost:9000/airlinesmall/*", 1).toJavaRDD();
		
		
		JavaRDD<String[]> filtercancelled= airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[21] != null && !linetosplit[21].equals("NA")
						&& !linetosplit[21].equals("Cancelled"));
		
		JavaPairRDD<String, Long> totalcancelled = filtercancelled.mapToPair(line -> new Tuple2<String,Long>(line[8], Long.parseLong(line[21]))).reduceByKey((a,b)->a+b);
		
		JavaPairRDD<String, Long> numofrecordscancelled = filtercancelled
				.mapToPair(line -> new Tuple2<String,Long>(line[8], 1l)).reduceByKey((a,b)->a+b);
		
		JavaRDD<String> carriers = sc.textFile("hdfs://localhost:9000/carriers/*", 1).toJavaRDD();

		JavaPairRDD<String, String> carriersinfo = carriers.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[0] != null && !linetosplit[0].equals("Code"))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));
		
		JavaPairRDD<String, Tuple2<Tuple2<Long, String>, Long>> combinecancelled = totalcancelled.join(carriersinfo).join(numofrecordscancelled);
		
		combinecancelled.saveAsTextFile("hdfs://localhost:9000/airlinecarriers/airlinecancelled" + System.currentTimeMillis());
	}

}
