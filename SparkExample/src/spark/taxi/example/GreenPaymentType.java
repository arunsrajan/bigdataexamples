package spark.taxi.example;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class GreenPaymentType {

	public static void main(String[] args) {
		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxigreen = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/green_tripdata_2018-12.csv");

		JavaPairRDD<Long,Long> summary = taxigreen.toJavaRDD().mapToPair(dat->new Tuple2<Long,Long>(Long.parseLong(dat.getAs("payment_type")),1l));

		List<Tuple2<Long,Long>> datas=summary.reduceByKey((a,b)->a+b).collect();

		datas.stream().forEach(dat->System.out.println(dat._1+" "+dat._2));
		
		System.out.println(datas);
		
	}

}
