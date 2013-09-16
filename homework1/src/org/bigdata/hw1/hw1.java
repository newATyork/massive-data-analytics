package org.bigdata.hw1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.bigdata.hw1.FileDownload;




public class hw1 {

	public static void main(String[] args) 
		throws IOException,InterruptedException, ClassNotFoundException {
		
		String res1 = FileDownload.downloadFromUrl("http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-coordinates.csv","/usr/local/hadoop/");  
	    System.out.println(res1);   
	        
	    String res2 = FileDownload.downloadFromUrl("http://vgc.poly.edu/~juliana/courses/cs9223/Homework/HW1/hurricane-center-addresses.csv","/usr/local/hadoop/");  
	    System.out.println(res2); 


		Configuration conf = new Configuration();
		Job job = new Job(conf, "Big Data HomeWork1");	
		
		
		
		FileSystem hdfs = FileSystem.get(conf);
		//hdfs.mkdirs(new Path("/user/hduser/addr")); 
	    Path src = new Path("/usr/local/hadoop/hurricane-center-addresses.csv");
	    Path dst = new Path("/user/hduser/addr/addresses.csv");
	    
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close(); 
        
        
        FileSystem hdfs2 = FileSystem.get(conf);
	    Path src2 = new Path("/usr/local/hadoop/hurricane-center-coordinates.csv");
	    Path dst2 = new Path("/user/hduser/addr/coordinates.csv");
	    
        hdfs2.copyFromLocalFile(src2, dst2);
        hdfs2.close(); 
		
        
		
		 if( args[2].toUpperCase().equals("A") )
	     {
			 //    Job A: Select zipcode<10030
			 job.setJarByClass(hw1.class);
			 job.setMapperClass(TaskAMapper.class);
			 job.setOutputKeyClass(IntWritable.class);
			 job.setOutputValueClass(Text.class);
			 FileInputFormat.addInputPath(job, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 //job.waitForCompletion(true);
	     } 
		 
		 
		 if( args[2].toUpperCase().equals("B") )
	     {
			//  Job B: eliminate duplicates
			 job.setJarByClass(hw1.class);
			 job.setMapperClass(TaskBMapper.class);
		   	 job.setReducerClass(TaskBReducer.class);
		     job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(IntWritable.class);
			 FileInputFormat.addInputPath(job, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 //job.waitForCompletion(true);
	     } 
		
		
		 if( args[2].toUpperCase().equals("C") )
	     {
			//  Job C: Addresses Natural Join Coordinates 
			job.setJarByClass(hw1.class);
			job.setMapperClass(TaskCMapper.class);
			job.setReducerClass(TaskCReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			//job.waitForCompletion(true);
	     }
		 
		 job.waitForCompletion(true);

	}
}
