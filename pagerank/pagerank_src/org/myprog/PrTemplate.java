package org.myprog;

//import java.io.File;
import java.util.*;
//import java.io.*;
import java.io.IOException;
//import java.io.FileReader;

            
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
       
public class PrTemplate {
       
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		//private final static IntWritable one = new IntWritable(1);
		private Text word1 = new Text();
		private Text word2 = new Text();
           
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String delimiter = ",";
			//String delimiter = "\t";
			String str1,str2; 
			StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
			
			while (tokenizer.hasMoreTokens()) {
				
				str1 = tokenizer.nextToken();
				//str1 = str1.trim();
				str1 = str1.trim()+" , ";
				word1.set(str1);
				if(tokenizer.hasMoreTokens()) {
					str2 = tokenizer.nextToken();
					word2.set(str2);
				}
				context.write( word1, word2 );
			}
       }
    } 
           
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		
    		//int sum = 0;
    		StringBuffer AList = new StringBuffer();
    		
    		AList.append(" 10000.0 ,");
    		for (Text val : values) {
    			AList.append(" ");
    			AList.append(val.toString());
    			AList.append(",");
    			//sum ++;
    		}
    		
    		AList.deleteCharAt(AList.length()-1);
    		
    		//AList.append("; ");
    		//AList.append(sum);
    				
    		context.write(key, new Text(AList.toString()));
    		AList.delete(0,AList.length()-1);
    	}
    }
           
    public static void main(String[] args) throws Exception {
    	
	    Configuration conf = new Configuration();
	       
	    Job job = new Job(conf, "PageRank Template");
		job.setJarByClass(PrTemplate.class);
	      
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	           
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	           
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	           
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	           
	    job.waitForCompletion(true);
	       
	}
           
}