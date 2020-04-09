import org.crunchy.mdc.stream.MassiveDataPipeline;


public class MassiveDataCollectExample6 {
	static String airline = "/airlinesmall";
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
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
	@SuppressWarnings({ "rawtypes" })
	public static void testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered() throws Throwable {
		System.out.println("testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered Before---------------------------------------");
		MassiveDataPipeline<String,String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline);
		MassiveDataPipeline mapColumnsMonth6_7= (MassiveDataPipeline) datastream.csvWithHeader(airlineheader,false)
				.sql("SELECT UniqueCarrier,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,"
						+ "TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,Month "
						+ "FROM MyTable WHERE Month <> 'Month' AND (Month = 10 OR Month = 11)");
		mapColumnsMonth6_7.saveAsTextFile("hdfs://127.0.0.1:9000/newmapperout/MapRed-"+System.currentTimeMillis());
		System.out.println("testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered After---------------------------------------");
	}
}
