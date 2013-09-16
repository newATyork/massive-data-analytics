package org.myprog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;

/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner; */
//import org.apache.hadoop.io.WritableComparable;

public class PageRank {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		//String pathIn = "hdfs://localhost:54310/user/hduser/yuanxing";
		//String pathIn = "hdfs://localhost:54310/user/hduser/pagerankdir";
		
		
		//String pathIn = "hdfs://localhost:54310/user/hduser/yuanxingb";
		
		//String pathIn = "/user/hduser/pr_test";
		
		//String pathIn = "s3n://big.data.moban/go";
		String pathIn = "s3n://pagerankint/int";
		//String pathIn = "s3n://big.data.normtemp/out2";
		//String pathIn = "hdfs://localhost:54310/user/hduser/pagerankdir";
		//String pathIn = "hdfs://localhost:54310/user/hduser/pr_f";
		
		String pathOut = "";
		
		for (int i = 0; i < 10; i++) {
			System.out.println("iteration id=" + i);
			Job job = new Job(conf, "MapReduce PageRank");
			
			pathOut = pathIn + i;
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(pathIn));
			FileOutputFormat.setOutputPath(job, new Path(pathOut));
			pathIn = pathOut;
			job.waitForCompletion(true);
		}
		
		//=============================Sorting PageRank ======================================
		
		
		Configuration conf2 = new Configuration();
		String new_pathin = pathOut;
		String new_pathout = new_pathin + "_final";
		Job sortjob = new Job(conf2, "Sorted PageRank");
		
		sortjob.setJarByClass(PageRank.class);
		sortjob.setMapperClass(RankSortMapper.class);
		sortjob.setReducerClass(RankSortReducer.class);
		//sortjob.setOutputKeyClass(FloatWritable.class);
		sortjob.setOutputKeyClass(LongWritable.class);
		sortjob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(sortjob, new Path(new_pathin));
		FileOutputFormat.setOutputPath(sortjob, new Path(new_pathout));
		sortjob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		sortjob.waitForCompletion(true);
		
		
	}
}