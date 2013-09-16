package org.myprog;



import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankSortMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text values, Context context) {

		String newline = values.toString();
		String delimiter = ",";
		Text source_title, output_value;  //, pagerank;
		LongWritable PR = new LongWritable();	
		//FloatWritable PR = new FloatWritable();	
		float temp_float;
		Float fObj;
		
		StringTokenizer tokenizer = new StringTokenizer(newline,delimiter);
		
		if (tokenizer.hasMoreTokens()) {
			// fetch source_title
			source_title = new Text(tokenizer.nextToken().trim());
		} else {
			return;
		}
		
		
		if (tokenizer.hasMoreTokens()) {
			// fetch pagerank
			//pagerank = new Text(tokenizer.nextToken().trim());
			
			temp_float = Float.parseFloat( tokenizer.nextToken().trim() );
			
			fObj = new Float(temp_float); 

			
			PR.set( fObj.intValue() );
			//PR.set(temp_float);
			
			
		} else {
			return;
		}
		
		String temp = "";
		
		
		try {
			while (tokenizer.hasMoreTokens()) { 
				// fetch target_titles
			temp= temp + tokenizer.nextToken().trim();
			temp = temp + " , ";
			}
			
			//temp = temp.substring(0,temp.length()-2);
			//temp = "   target_pages are : " + temp ;
			
			temp = source_title.toString() +" , "+ temp;
			
			output_value = new Text(temp);
			
			context.write( PR, output_value);
			
			
				
		} catch (IOException e) {
				e.printStackTrace();
		} catch (InterruptedException e) {
				e.printStackTrace();
		}
			
	}
}