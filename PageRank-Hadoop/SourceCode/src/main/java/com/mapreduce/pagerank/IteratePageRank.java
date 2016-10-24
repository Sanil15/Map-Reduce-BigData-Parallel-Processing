package com.mapreduce.pagerank;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class IteratePageRank {
	
	// This is a Dummy page name which helps to identify DanglingNode that is sent from Mapper to reducer
	public static final Text DANGLINGNODEIDENTIFIER = new Text("&~DANGLING_IDENTIFIER#DUMMY_INVALID_KEY~&");
	// Probablity of Random Jump
	public static final Double DAMPINGFACTOR = 0.15;
	
	
	public static Double getDampingfactor() {
		return DAMPINGFACTOR;
	}

	// Global counter for maintaining Dangling Node contributions
	public static enum UpdateCounter{
		DANGLINGOFFSET;
	}

	// Global counter for maintaining the Page Name count
	public static enum UpdatePageNameCounter{
		PAGENAMECOUNTER;
	}
	
	
	// Method that run the IteratePageRank Job and returns the Job object for the ease of
	// access of Global Counters for that job anywhere function gets invoked from.
	public static Job runIteratePageRank(String[] args, Configuration conf) throws Exception {

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Iteration Page Rank");
		job.setJarByClass(IteratePageRank.class);
		job.setMapperClass(IteratePageRankMapper.class);
		job.setReducerClass(IteratePageRankReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));

		job.waitForCompletion(true);
		
		return job;

	}	


}
