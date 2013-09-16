package org.bigdata.hw1;

import java.io.IOException;
//import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskBReducer extends Reducer<Text, IntWritable, Text, NullWritable>{

		    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		    	
		        context.write(key, NullWritable.get()); 
		    }

}
