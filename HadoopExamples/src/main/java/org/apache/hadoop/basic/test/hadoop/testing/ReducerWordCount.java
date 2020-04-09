package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWordCount extends Reducer<Text, IntWritable, Text, IntWritable>{
	protected void reduce(Text text, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
       for(IntWritable intWritable:values) {
    	   sum+=intWritable.get();
       }
       context.write(text, new IntWritable(sum));
    }
}
