package spark.taxi.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class YellowTollAmount {

	public static void main(String[] args) {
		
		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxiyellow = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/yellow_tripdata_2018-12.csv");

		Dataset<Double> sumaverage = taxiyellow.map(dat->Double.parseDouble(dat.getAs("tolls_amount")),Encoders.DOUBLE());

		sumaverage.summary().show();
		
	}

}
