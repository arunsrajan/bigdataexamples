package spark.taxi.example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class YellowPaymentType {

	public static void main(String[] args) {
		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxiyellow = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/yellow_tripdata_2018-12.csv");

		JavaPairRDD<Long,Long> summary = taxiyellow.toJavaRDD().mapToPair(dat->new Tuple2<Long,Long>(Long.parseLong(dat.getAs("payment_type")),1l));

		summary.reduceByKey((a,b)->a+b).foreach(dat->System.out.println(dat._1+" "+dat._2));

	}

}
