package org.myprog;

import java.io.IOException;
import java.util.StringTokenizer;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text source_title;
	// value of PR
	private String pr;
	// Num of Out_links
	private int count;
	// average PR
	private float averagePr;
	

	public void map(LongWritable key, Text value, Context context) {
		
		
		String newline = value.toString();
		String delimiter = ",";
		
		StringTokenizer tokenizer = new StringTokenizer(newline,delimiter);
		
		if (tokenizer.hasMoreTokens()) {
			// fetch source_title
			source_title = new Text(tokenizer.nextToken().trim());
		} else return;
		// fetch PR
		if (tokenizer.hasMoreTokens()) {
			// fetch source_title
			pr = tokenizer.nextToken().trim();
		}
		
		count = tokenizer.countTokens();
		
		averagePr = Float.parseFloat(pr) / count;
		
		
		while (tokenizer.hasMoreTokens()) { 
			try { 
				String next_page = tokenizer.nextToken().trim();
					
				Text t1 = new Text("@" + averagePr); 
				context.write(new Text(next_page), t1);
				
					
				Text tt = new Text("&" + next_page);
				context.write(source_title, tt);  
					
			} catch (IOException e) {
					e.printStackTrace();
			} catch (InterruptedException e) {
					e.printStackTrace();
			}
		} 
	}
}