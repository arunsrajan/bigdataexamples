package test.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class ArrivalDelayUnion {

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		//sparkconf.set("spark.driver.supervise","true");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airlines = sc.textFile("hdfs://localhost:9000/airlineverysmall/*", 1).toJavaRDD();
		
		JavaRDD<String[]> filterdataarrdelay= airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[14] != null && !linetosplit[14].equals("NA")
						&& !linetosplit[14].equals("ArrDelay"));
				
		JavaPairRDD<String, Long> carrierarrvdelay = filterdataarrdelay.mapToPair(line -> new Tuple2<String,Long>(line[8], Long.parseLong(line[14]))).reduceByKey((a,b)->a+b);
		
		
		airlines = sc.textFile("hdfs://localhost:9000/airlineveryverysmall/*", 1).toJavaRDD();
		
		JavaRDD<String[]> filterdataarrdelay1 = airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[14] != null && !linetosplit[14].equals("NA")
						&& !linetosplit[14].equals("ArrDelay"));
		
		
		List<String[]> arrdelayList = filterdataarrdelay.union(filterdataarrdelay1).collect();
		
		System.out.println(arrdelayList.size());
		for (String[] cardelay : arrdelayList) {
			System.out.println(cardelay);
		}
		carrierarrvdelay.saveAsTextFile("hdfs://localhost:9000/airlinecarriers/arrdelay" + System.currentTimeMillis());
	}

}
