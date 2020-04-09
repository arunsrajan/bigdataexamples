package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class StateAirport {

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		System.out.println(sparkconf);
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airports = sc.textFile("hdfs://localhost:9000/airport/*", 1).toJavaRDD();

		JavaPairRDD<String, String> airportsbystate= airports.map(linetosplit -> linetosplit.split(",")).mapToPair(line -> new Tuple2<String,String>(line[3], line[1])).sortByKey();
		

		airportsbystate.saveAsTextFile("hdfs://localhost:9000/airportbystate/stateairports"+System.currentTimeMillis());
		
	}

}
