package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SplitDataMRJob extends Configured implements Tool {

	public static class AirlineSplitMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		MultipleOutputs mos=null;
		
		public void setup(Context ctx) {
			mos = new MultipleOutputs(ctx);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 if(!AirlineDataUtils.isHeader(value)){         
		           String[] contents = value.toString().split(",");
		           String flightArrivalDelay=AirlineDataUtils.getArrivalDelay(contents);
		           String carrierCode = AirlineDataUtils.getUniqueCarrier(contents);
		           
		           if(carrierCode.equals("PI")) {
		        	   mos.write("PI", NullWritable.get(), value);
		           }
		           else if(carrierCode.equals("PS")) {
		        	   mos.write("PS", NullWritable.get(), value);
		           }
		           else if(carrierCode.equals("TW")) {
		        	   mos.write("TW", NullWritable.get(), value);
		           }
		           else {
		        	   mos.write("Others", NullWritable.get(), value);
		           }
			 }
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
	}
	
	
	@Override
	public int run(String[] allArgs) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SplitDataMRJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "PI", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "PS", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "TW", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "Others", TextOutputFormat.class, NullWritable.class, Text.class);
		job.setMapperClass(AirlineSplitMapper.class);
	
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJobName("SplitterJob");
		job.setNumReduceTasks(0);

		
		String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);

		if (status) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SplitDataMRJob(), args);

	}

}
