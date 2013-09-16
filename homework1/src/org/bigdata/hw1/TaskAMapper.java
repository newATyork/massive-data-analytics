package org.bigdata.hw1;

import java.io.IOException;
//import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskAMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context) {
		
		String newline = value.toString();
		String delimiter = ",";
		String str;
		int zip;
		IntWritable ZipCode = new IntWritable(-1);
		Text outputline = new Text(newline);
		
		String[] array = value.toString().split(delimiter);
		
		str=array[array.length - 2];
		
		if(str.contains("ZIP_CODE"))
			return;
		
	    zip = Integer.parseInt(str);

		//StringTokenizer tokenizer = new StringTokenizer(newline, delimiter);
		//There are commas in last 4 records's attribute, numeric matching  errors occur. Stringtokenizer cannot be used.
		
		try {

			if(zip >= 10030)
					return;
			else
			{
					ZipCode.set(zip);
					context.write(ZipCode , outputline);
			}
				
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
	
			
		
	
