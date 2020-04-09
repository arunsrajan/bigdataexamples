package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class TotalFlightDistance {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airlines = sc.textFile("hdfs://localhost:9000/airline2/*", 1).toJavaRDD();
		
		
		JavaRDD<String[]> filterdistance= airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[18] != null && !linetosplit[18].equals("NA")
						&& !linetosplit[18].equals("Distance"));
		
		JavaPairRDD<String, Long> totaldistance = filterdistance.mapToPair(line -> new Tuple2<String,Long>(line[8], Long.parseLong(line[18]))).reduceByKey((a,b)->a+b);
		
		JavaPairRDD<String, Long> numofrecordsdistance = filterdistance
				.mapToPair(line -> new Tuple2<String,Long>(line[8], 1l)).reduceByKey((a,b)->a+b);
		
		JavaRDD<String> carriers = sc.textFile("hdfs://localhost:9000/carriers/*", 1).toJavaRDD();

		JavaPairRDD<String, String> carriersinfo = carriers.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[0] != null && !linetosplit[0].equals("Code"))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));
		
		JavaPairRDD<String, Tuple2<Tuple2<Long, String>, Long>> combinedistance = totaldistance.join(carriersinfo).join(numofrecordsdistance);
		
		combinedistance.saveAsTextFile("hdfs://localhost:9000/airlinecarriers/secdelay" + System.currentTimeMillis());
	}

}
