package test.spark;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class UniqueCarrierDayOfMonth {

	public static void main(String[] args) {
SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);

		JavaRDD<String> airlines = sc.textFile("hdfs://localhost:9000/airlinesmall/*", 1).toJavaRDD();

		JavaRDD<String[]> filterdayofmonth= airlines.map(linetosplit -> linetosplit.split(","))
				.filter(linetosplit -> linetosplit[2] != null && !linetosplit[2].equals("NA")
						&& !linetosplit[2].equals("DayOfMonth"));
				
		JavaPairRDD<String, Long> carrierdayofmonth = filterdayofmonth.mapToPair(line -> new Tuple2<String,Long>(line[8]+"-"+line[2], 1l)).reduceByKey((a,b)->a+b);

		carrierdayofmonth.mapToPair(item -> item.swap()).sortByKey().saveAsTextFile("hdfs://localhost:9000/airlinecarriers/carrierdayofmonth" + System.currentTimeMillis());
		
	}
	class TupleComparator implements Comparator<Tuple2<Long, String>>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8284721829597074064L;

		@Override
		public int compare(Tuple2<Long, String> o1, Tuple2<Long, String> o2) {
		    return (int) (o1._1 - o2._1);
		  }
		}
}
