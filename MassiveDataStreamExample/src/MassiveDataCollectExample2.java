import org.apache.commons.csv.CSVRecord;
import org.crunchy.mdc.stream.MapTuple;
import org.crunchy.mdc.stream.MassiveDataPipeline;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;


public class MassiveDataCollectExample2 {
	static String airline = "/airline";
	static String[] airlineheader = new String[] {"Year","Month","DayofMonth","DayOfWeek","DepTime"
			,"CRSDepTime",
			"ArrTime","CRSArrTime","UniqueCarrier","FlightNum","TailNum","ActualElapsedTime","CRSElapsedTime","AirTime",
			"ArrDelay","DepDelay","Origin","Dest",
			"Distance","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay",
			"NASDelay","SecurityDelay","LateAircraftDelay"};
	static String[] carrierheader = {"Code","Description"};
	public static void main(String[] args) throws Throwable {
		testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered();
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered() throws Throwable {
		System.out.println("testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered Before---------------------------------------");
		MassiveDataPipeline datastream = MassiveDataPipeline.newStreamHDFS("hdfs://127.0.0.1:9000", airline);
		MapTuple<Tuple,String> map= (MapTuple<Tuple,String>) datastream.csvWithHeader(airlineheader,false)
				.sql("SELECT UniqueCarrier,ArrDelay FROM MyTable WHERE ArrDelay <> 'NA' and ArrDelay <> 'ArrDelay'");
		MapTuple<Tuple,Long> mapArrivalDelay = (MapTuple) map
				.mapTuple((Tuple tup)->Tuple.tuple(((Tuple2)tup).v1, new Long((String)((Tuple2)tup).v2))).reduceByKey((a,b)->(Long)a+ (Long)b,null);
		
		MassiveDataPipeline datastreamcarriers = MassiveDataPipeline.newStreamHDFS("hdfs://127.0.0.1:9000", "/carriers");
		MapTuple<CSVRecord,String> mapCarriers= (MapTuple<CSVRecord, String>) datastreamcarriers.csvWithHeader(carrierheader,false)
				.sql("SELECT Code,Description FROM MyTable WHERE Code <> 'Code'");
		mapArrivalDelay.join(mapCarriers, (tuple1,tuple2)->((Tuple2)tuple1).v1.equals(((Tuple2)tuple2).v1)).saveAsTextFile("hdfs://127.0.0.1:9000/newmapperout/MapRed-"+System.currentTimeMillis());
		System.out.println("testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered After---------------------------------------");
	}
}
