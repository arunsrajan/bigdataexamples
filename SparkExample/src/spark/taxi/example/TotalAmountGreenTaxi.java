package spark.taxi.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TotalAmountGreenTaxi {

	public static void main(String[] args) {

		SparkSession ss = SparkSession.builder().master("local[*]").getOrCreate();

		Dataset<Row> taxiGreen = ss.read().option("header", true).csv("hdfs://localhost:9000/traveldata/2018/green_tripdata_2018-12.csv");
		
		Dataset<Double> datas = taxiGreen.map(row->Double.parseDouble(row.getAs("total_amount")),Encoders.DOUBLE());
		
		datas.groupBy().sum().show();
		
		datas.summary().show();
		
	}

}
