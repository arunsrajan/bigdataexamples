package test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;;

public class AssociationRulesExample {

	public static void main(String[] args) {
		SparkConf sparkconf = new SparkConf();
		
		sparkconf.set("spark.master", "local");
		
		sparkconf.setAppName("SparkAirlineApp");
		
		SparkContext sc = SparkContext.getOrCreate(sparkconf);
		JavaRDD<String> data = sc.textFile("hdfs://127.0.0.1:9000/airlinesample/*", 1).toJavaRDD();
		JavaRDD<Vector> parsedData = data.map(dat->dat.split(",")).filter(dat->!dat[0].equals("NA")&&
				!dat[1].equals("NA")&&
				!dat[2].equals("NA")&&
				!dat[3].equals("NA")&&
				!dat[4].equals("NA")&&
				!dat[5].equals("NA")&&
				!dat[6].equals("NA")&&
				!dat[7].equals("NA")&&
				!dat[9].equals("NA")).map(sarray -> {
			  double[] doub = {new Double(sarray[0]).doubleValue(),
					  new Double(sarray[1]).doubleValue(),new Double(sarray[2]).doubleValue(),
					  new Double(sarray[3]).doubleValue(),new Double(sarray[4]).doubleValue()
					  ,new Double(sarray[5]).doubleValue(),new Double(sarray[6]).doubleValue()
					  ,new Double(sarray[7]).doubleValue(),new Double(sarray[9]).doubleValue()};
			  return Vectors.dense(doub);
			});
			parsedData.cache();
			
			// Cluster the data into two classes using KMeans
			int numClusters = 31;
			int numIterations = 20;
			KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

			System.out.println("Cluster centers:");
			for (Vector center: clusters.clusterCenters()) {
			  System.out.println(" " + center);
			}
			System.out.println(clusters.predict( Vectors.dense(2007,12,31,1,1359,1405,1427,1434,26)));
			double cost = clusters.computeCost(parsedData.rdd());
			System.out.println("Cost: " + cost);

			// Evaluate clustering by computing Within Set Sum of Squared Errors
			double WSSSE = clusters.computeCost(parsedData.rdd());
			System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

	}

}
