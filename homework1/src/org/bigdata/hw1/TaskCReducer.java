package org.bigdata.hw1;

import java.io.IOException;
//import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaskCReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterable<Text> values, Context context) {
		
		//ArrayList<String> column = new ArrayList<String>();
		
		String addresses ="";
		String coodinates ="";
		
		for (Text val : values) {
			String temp = val.toString();
			
			if (temp.substring(0, 2).equals("\"<")) {
				
				coodinates=temp;
			} 
			else  {
				addresses=temp;
			}
		}
		
		String newValue = addresses + "        "+ coodinates;
		
		try {
			context.write(key, new Text(newValue));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}