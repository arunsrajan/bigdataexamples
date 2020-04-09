package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinMRJob extends Configured implements Tool {

	static Text carrier = new Text("CARRIER");
	static Text arrivalDelay = new Text("ARRIVALDELAY");
	static Text arrivalDelaySum = new Text("ARRIVALDELAYSUM");
	public static class CarriersMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 if(!AirlineDataUtils.isHeader(value)){         
		           String[] contents = value.toString().split(",");
		           context.write(new Text(contents[0].substring(1,contents[0].length()-1)), getMapWritable(carrier,new Text(contents[1])) );
			 }
		}

	}

	private static MapWritable getMapWritable(Text type,Writable value){
        MapWritable map = new MapWritable();
        map.put(type, value);
        return map;
    }
	public static class AirlineMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 if(!AirlineDataUtils.isHeader(value)){         
		           String[] contents = value.toString().split(",");
		           String flightArrivalDelay=AirlineDataUtils.getArrivalDelay(contents);
		           if(flightArrivalDelay!=null && !flightArrivalDelay.equals("NA")) {
		        	   context.write(new Text(AirlineDataUtils.getUniqueCarrier(contents)), getMapWritable(arrivalDelay,new LongWritable(Long.parseLong(flightArrivalDelay))) );
		           }
			 }
		}

	}
	
	public static class AirlineCarrierCombine extends Reducer<Text, MapWritable, Text, MapWritable> {
	     public void reduce(Text key, Iterable<MapWritable> values, Context context)
	   throws IOException, InterruptedException {
	    	 long sum=0;
	    	 Text carrierName= new Text();
	    	 LongWritable delay;
	    	 for(MapWritable map:values) {
	    		 if(map.get(carrier)!=null) {
	    			 carrierName=(Text) map.get(carrier); 
	    			 context.write(key,  map);
	    		 }
	    		 else if(map.get(arrivalDelay)!=null) {
	    			 delay = (LongWritable) map.get(arrivalDelay);
	    			 sum+=delay.get();
	    		 }
	    	 }
	    	 if(sum!=0) {
	    		 context.write(key,  getMapWritable(arrivalDelay,new LongWritable(sum)));
	    	 }
	     }
	 }
	
	 public static class AirlineCarrierJoin extends Reducer<Text, MapWritable, Text, Text> {
	     public void reduce(Text key, Iterable<MapWritable> values, Context context)
	   throws IOException, InterruptedException {
	    	 long sum=0;
	    	 Text carrierName= new Text();
	    	 LongWritable delay;
	    	 for(MapWritable map:values) {
	    		 if(map.get(carrier)!=null) {
	    			 carrierName=(Text) map.get(carrier); 
	    		 }
	    		 else if(map.get(arrivalDelay)!=null) {
	    			 delay = (LongWritable) map.get(arrivalDelay);
	    			 sum+=delay.get();
	    		 }
	    	 }
	    	 if(sum>0) {
	    		 context.write(key,  new Text(carrierName.toString()+","+sum+""));
	    	 }
	     }
	 }
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new JoinMRJob(), args);
	}

	@Override
	public int run(String[] allArgs) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(JoinMRJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setCombinerClass(AirlineCarrierCombine.class);
		
		job.setReducerClass(AirlineCarrierJoin.class);
		job.setJobName("JoiningJob");
		//job.setNumReduceTasks(1);

		
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
		System.out.println(Arrays.asList(args));
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CarriersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirlineMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		boolean status = job.waitForCompletion(true);

		if (status) {
			return 0;
		} else {
			return 1;
		}
	}

}
