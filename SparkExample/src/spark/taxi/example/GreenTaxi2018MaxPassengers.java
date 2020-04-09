package spark.taxi.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class GreenTaxi2018MaxPassengers {

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> taxiyellow = sc.textFile("hdfs://localhost:9000/traveldata/2018/green_tripdata_2018-12.csv", 1).toJavaRDD();

		Tuple2<Long, String> tup = taxiyellow.map(line->line.split(",")).filter(dat->dat.length>7&&!dat[7].equals("passenger_count")).
		mapToPair(linarr->new Tuple2<Long,String>(Long.parseLong(linarr[7]),linarr[1]+"-"+linarr[2])).sortByKey(false).
		first();
		
		System.out.println(tup);
		
	}

}
