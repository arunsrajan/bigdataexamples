import java.util.List;

import org.crunchy.mdc.stream.MassiveDataPipeline;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;


public class TotalCarriers {
	static String airline = "/airlinesmall";
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	
	public static void main(String[] args) throws Throwable {
		testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered();
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void testCsvStreamSqlArrivalDelayCarrierRightOuterJoinCodeFiltered() throws Throwable {
		System.setProperty("taskschedulerstream.blocksize", "10");
		MassiveDataPipeline<String,String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airline);
		List<List<Tuple2>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.mapTuple(dat -> (Tuple2<String,Long>)Tuple.tuple(dat[8], 1l))
				.reduceByKey((pair1, pair2) -> (Long)pair1 + (Long)pair2,(pair1, pair2) -> (Long)pair1 + (Long)pair2)
				.sorted((val1,val2)->{
					Tuple2 tup1 = (Tuple2)val1;
					Tuple2 tup2 = (Tuple2)val2;
					
						return ((String)tup1.v1)
								.compareToIgnoreCase(((String)tup2.v1));
					
				}).peek(System.out::println)				
				.collect(true,null);
		redByKeyList.stream().flatMap(stream->stream.stream()).forEach(System.out::println);
	}
}
