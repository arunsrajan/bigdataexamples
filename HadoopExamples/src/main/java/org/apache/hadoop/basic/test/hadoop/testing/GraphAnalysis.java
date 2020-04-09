package org.apache.hadoop.basic.test.hadoop.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GraphAnalysis extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GraphAnalysis(), args);
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

		job.setMapperClass(GraphAnalyzerTokenizer.class);
		//job.setCombinerClass(GraphAnalysisReducer.class);
		job.setReducerClass(GraphAnalysisReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

}
