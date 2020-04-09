package org.apache.hadoop.basic.test.hadoop.testing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphAnalyzerTokenizer extends 
Mapper<
Object, 
Text, 
Text, 
LongWritable> {
	protected void map(Object offset, Text value, Context context) 
            throws IOException, InterruptedException {
		System.out.println("value:"+value.toString());
        StringTokenizer tok = new StringTokenizer(value.toString(),"\t");
        String source=tok.nextToken();
        String target=tok.nextToken();
        System.out.println(source+" "+target);
        context.write(new Text(source), new LongWritable(new Long(target)));
    }
}
