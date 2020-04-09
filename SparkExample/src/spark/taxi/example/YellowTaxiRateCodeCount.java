package spark.taxi.example;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class YellowTaxiRateCodeCount {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxiyellowratecode = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/yellow_tripdata_2018-12.csv");

		JavaPairRDD<Long,Long> dataratecode = taxiyellowratecode.toJavaRDD().mapToPair(dat->new Tuple2<Long,Long>(Long.parseLong(dat.getAs("RatecodeID")),1l)).reduceByKey((a,b)->a+b);
		List datalist = dataratecode.sortByKey().collect();
		datalist.forEach(System.out::println);
		
	}

}
