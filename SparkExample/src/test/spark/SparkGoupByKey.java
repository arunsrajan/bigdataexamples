package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class SparkGoupByKey {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkGoupByKey");

		SparkContext sc = SparkContext.getOrCreate(sparkconf);
		
		JavaRDD<String> airlines = sc.textFile("hdfs://localhost:9000/airlinemedium/*", 1).toJavaRDD();

		JavaPairRDD<String, Long> carrierarrvdelay = airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[14] != null && !linetosplit[14].equals("NA")
						&& !linetosplit[14].equals("ArrDelay"))
				.mapToPair(line -> new Tuple2(line[8], Long.parseLong(line[14])));

		JavaPairRDD<String, Iterable<Long>> groupbykey = carrierarrvdelay.groupByKey();
		groupbykey.sortByKey().saveAsTextFile("hdfs://localhost:9000/sparkresult/test-"+System.currentTimeMillis());
		sc.stop();
	}

}
