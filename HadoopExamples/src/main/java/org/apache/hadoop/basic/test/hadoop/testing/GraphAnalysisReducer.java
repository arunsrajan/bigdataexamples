package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphAnalysisReducer extends Reducer<Text, LongWritable, Text, Text>{
	protected void reduce(Text text, Iterable<LongWritable> values, Context context) 
            throws IOException, InterruptedException {
       String value="";
       for(LongWritable longWritable:values) {
    	   value+=text.toString()+"->"+longWritable.get()+",";
       }
       context.write(text, new Text(value.substring(0, value.length()-1)));
    }
}
