package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WritableComparableSortingJob extends Configured implements Tool {

	public static class SortingMapper extends Mapper<LongWritable, Text, MonthDoWWritable, DelaysWritable> {

		@Override
		public void map(LongWritable lw, Text txt, Context context) throws IOException, InterruptedException {
			if (!AirlineDataUtils.isHeader(txt)) {
				String contents[] = txt.toString().split(",");
				String month = AirlineDataUtils.getMonth(contents);
				String dayOfWeek = AirlineDataUtils.getDayOfTheWeek(contents);
				MonthDoWWritable monthDoWWritable = new MonthDoWWritable();
				monthDoWWritable.dayOfWeek.set(new Integer(dayOfWeek));
				monthDoWWritable.month.set(new Integer(month));
				DelaysWritable data = AirlineDataUtils.parseDelaysWritable(txt.toString());
				context.write(monthDoWWritable, data);
			}
		}

	}

	public static class SortAscMonthDescWeekReducer
			extends Reducer<MonthDoWWritable, DelaysWritable, NullWritable, Text> {
		public void reduce(MonthDoWWritable key, Iterable<DelaysWritable> values, Context context)
				throws IOException, InterruptedException {

			for (DelaysWritable val : values) {
				context.write(NullWritable.get(), new Text(AirlineDataUtils.parseDelaysWritableToText(val)));
			}
		}
	}

	public static class MonthDoWPartitioner extends Partitioner<MonthDoWWritable, DelaysWritable> implements Configurable {
		private Configuration conf = null;
		private int indexRange = 0;

		private int getDefaultRange() {
			int minIndex = 0;
			int maxIndex = 11 * 7 + 6;
			int range = (maxIndex - minIndex) + 1;
			return range;
		}

		// @Override
		public void setConf(Configuration conf) {
			this.conf = conf;
			this.indexRange = conf.getInt("key.range", getDefaultRange());
		}

		// @Override
		public Configuration getConf() {
			return this.conf;
		}

		public int getPartition(MonthDoWWritable key, DelaysWritable value, int numReduceTasks) {
			return AirlineDataUtils.getCustomPartition(key, indexRange, numReduceTasks);
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WritableComparableSortingJob(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration config= getConf();
		config.setBoolean("mapreduce.map.output.compress", true);
		config.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);
		
		Job job = new Job(config);
		job.setJarByClass(WritableComparableSortingJob.class);
		job.setMapOutputKeyClass(MonthDoWWritable.class);
		job.setMapOutputValueClass(DelaysWritable.class);
		job.setMapperClass(SortingMapper.class);
		job.setReducerClass(SortAscMonthDescWeekReducer.class);
		job.setPartitionerClass(MonthDoWPartitioner.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJobName("SortingJob");
		String[] arg = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.submit();
		job.waitForCompletion(true);
		return 0;
	}

}
