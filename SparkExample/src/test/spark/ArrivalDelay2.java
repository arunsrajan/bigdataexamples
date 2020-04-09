package test.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class ArrivalDelay2 {

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		sparkconf.set("spark.driver.supervise","true");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airlines = sc.textFile("hdfs://127.0.0.1:9000/airlinecomplete*/*", 1).toJavaRDD();
		
		JavaRDD<String[]> filterdataarrdelay= airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[14] != null && !linetosplit[14].equals("NA")
						&& !linetosplit[14].equals("ArrDelay"));
				
		JavaPairRDD<String, Long> carrierarrvdelay = filterdataarrdelay.mapToPair(line -> new Tuple2<String,Long>(line[8], Long.parseLong(line[14]))).reduceByKey((a,b)->a+b);
		
		List<Tuple2<String,Long>> arrdelayList = carrierarrvdelay.collect();
		
		int sum = 0;
		for (Tuple2<String,Long> pair : arrdelayList) {
			System.out.println(pair._1 + " " + pair._2);
			sum += (Long) pair._2;
		}
		System.out.println(sum);
		
		carrierarrvdelay.saveAsTextFile("hdfs://127.0.0.1:9000/airlinecarriers/arrdelay" + System.currentTimeMillis());
	}

}
