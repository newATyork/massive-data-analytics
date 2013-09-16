package org.myprog;

//package org.myprog;

import java.io.IOException;
import java.util.ArrayList;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends
		Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) {
		
		String str_key = key.toString();
		str_key = str_key + " ,";

		
		ArrayList<String> page_ids = new ArrayList<String>();
		
		String outLink = "  ,";
		
		float pr = 0;
		
		for (Text page_id_val : values) {
			String page_idd = page_id_val.toString();
			
			if (page_idd.substring(0, 1).equals("@")) {
				
				pr += Float.parseFloat(page_idd.substring(1));
			} else if (page_idd.substring(0, 1).equals("&")) {
				
				String iddd = page_idd.substring(1);

				page_ids.add(iddd);
			}
		}
		
		//pr = pr*0.85f + 0.15f * 0.25f;
		pr = pr * 0.85f + 10000*0.15f;
		
		for (int i = 0; i < page_ids.size(); i++) {
			outLink += page_ids.get(i) + "  ,";
		}
		
		outLink = outLink.substring(0,outLink.length()-1);
		
		String result = pr + outLink;

		try {
			context.write(new Text(str_key), new Text(result));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}