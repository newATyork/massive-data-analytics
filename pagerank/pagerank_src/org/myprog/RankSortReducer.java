package org.myprog;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class RankSortReducer extends
		Reducer< LongWritable , Text, LongWritable , Text> {
	public void reduce(LongWritable key, Iterable<Text> values, Context context) {
		
		String source_page_list="";
		
		for (Text page_id_val : values) {
			
			StringTokenizer tokenizer = new StringTokenizer( page_id_val.toString(), ",");
			
			if (tokenizer.hasMoreTokens()) {
				source_page_list = source_page_list + tokenizer.nextToken().trim(); //+ " , " +  ;
				source_page_list = source_page_list + " , ";
			}
		}
		
		source_page_list = source_page_list.substring(0,source_page_list.length()-2);
		
		try {
			context.write(key, new Text("Pages of this PageRank: " + source_page_list));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}