package spark.taxi.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GreenTollAmount {

	public static void main(String[] args) {
		
		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxigreen = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/green_tripdata_2018-12.csv");

		Dataset<Double> sumaverage = taxigreen.map(dat->Double.parseDouble(dat.getAs("tolls_amount")),Encoders.DOUBLE());

		sumaverage.summary().show();
		
	}

}
