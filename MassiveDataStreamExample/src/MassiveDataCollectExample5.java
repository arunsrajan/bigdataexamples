import org.crunchy.mdc.stream.MapTuple;
import org.crunchy.mdc.stream.MassiveDataPipeline;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;


public class MassiveDataCollectExample5 {
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	static String airline = "/airline";
	static String[] airlineheader = new String[] {"Year","Month","DayofMonth","DayOfWeek","DepTime"
			,"CRSDepTime",
			"ArrTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","ActualElapsedTime","CRSElapsedTime","AirTime",
			"ArrDelay","DepDelay","Origin","Dest",
			"Distance","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay",
			"NASDelay","SecurityDelay","LateAircraftDelay"};
	static String[] carrierheader = {"Code","Description"};
	public static void main(String[] args) throws Throwable {
		testCsvStreamSqlJoin();
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void testCsvStreamSqlJoin() throws Throwable {
		System.out.println("testCsvStreamSqlJoin Before---------------------------------------");
		MassiveDataPipeline<String,String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline);
		MapTuple<Tuple,String> map= (MapTuple<Tuple, String>) datastream.csvWithHeader(airlineheader,false)
				.sql("SELECT UniqueCarrier,ArrDelay FROM MyTable WHERE ArrDelay <> 'NA' and ArrDelay <> 'ArrDelay'");
		MapTuple<Tuple,Long> mapArrivalDelay = (MapTuple) map
				.mapTuple((Tuple tup)->Tuple.tuple(((Tuple2)tup).v1, new Long((String)((Tuple2)tup).v2))).reduceByKey((a,b)->(Long)a+ (Long)b,(a,b)->(Long)a+ (Long)b);
		
		MapTuple<Tuple,String> mapwithoutCoalesce = (MapTuple<Tuple,String>) datastream.csvWithHeader(airlineheader,false)
				.sql("SELECT UniqueCarrier,ArrDelay FROM MyTable WHERE ArrDelay <> 'NA' and ArrDelay <> 'ArrDelay'");
		MapTuple<Tuple,Long> mapwithoutCoalescereduce = (MapTuple) mapwithoutCoalesce
				.mapTuple((Tuple tup)->Tuple.tuple(((Tuple2)tup).v1, new Long((String)((Tuple2)tup).v2))).reduceByKey((a,b)->(Long)a+ (Long)b,null);
		mapArrivalDelay.join(mapwithoutCoalescereduce, (tuple1,tuple2)->((Tuple2)tuple1).v1.equals(((Tuple2)tuple2).v1))
				.saveAsTextFile("hdfs://127.0.0.1:9000/newmapperout/MapRed-"+System.currentTimeMillis());
		
		System.out.println("testCsvStreamSqlJoin After---------------------------------------");
	}
}
