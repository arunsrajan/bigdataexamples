package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CreatorCountTriples extends Configured implements Tool{
	public static class CreatorCountTriplesTokenizer extends 
	Mapper<
	Object, 
	Text, 
	Text, 
	IntWritable> {
		public final IntWritable one = new IntWritable(1);
		protected void map(Object offset, Text value, Context context) 
	            throws IOException, InterruptedException {
	        StringTokenizer tok = new StringTokenizer(value.toString(),",");
	        if(tok.hasMoreTokens()) {
		        tok.nextToken();
		        if(tok.hasMoreTokens()) {
			        String[] target=tok.nextToken().split("/");
			        String user = target.length!=0?target[target.length-1]:null;
			        if(user!=null)context.write(new Text(user), one);
		        }
	        }
	    }
	}
	
	public static class CreatorCountTriplesReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
		protected void reduce(Text text, Iterable<IntWritable> values, Context context) 
	            throws IOException, InterruptedException {
	       Long sum=0l;
	       for(IntWritable intWritable:values) {
	    	   sum+=intWritable.get();
	       }
	       context.write(text, new LongWritable(sum));
	    }
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CreatorCountTriples(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hadoop jar hadoop-testing-1.0.0-job.jar" + " [generic options] <in> <out>");
			System.out.println();
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}

		Job job = new Job(getConf(), "GraphAnalysis");
		job.setJarByClass(getClass());

		job.setMapperClass(CreatorCountTriplesTokenizer.class);
		//job.setCombinerClass(CreatorCountTriplesReducer.class);
		job.setReducerClass(CreatorCountTriplesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

}
