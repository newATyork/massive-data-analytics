package org.bigdata.hw1;

import java.io.IOException;
//import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskCMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context) {
		
		String newline = value.toString();
		String delimiter = ",";
		int id;
		IntWritable ID= new IntWritable(-1);
		
		String[] array = newline.toString().split(delimiter);
		
		if( array[0].equals("") )
			return;
		
		id = Integer.parseInt(array[0].trim());
		ID.set(id);
		
		String list = array[1].trim();
		
		for(int i =2; i<= array.length-1; i++)
			list = list + " , " + array[i].trim() ;
		
		try {
			context.write(ID , new Text(list));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
	