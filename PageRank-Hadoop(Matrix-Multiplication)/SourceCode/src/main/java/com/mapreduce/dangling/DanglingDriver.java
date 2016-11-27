package com.mapreduce.dangling;


import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


// 𝐌′𝐑(𝑡) =(𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡). For computing product 𝐃𝐑(𝑡), 
// encoding 𝐃 very compactly as the list of column numbers that have value 1/|V|.
// In our case D is 1 X |V| matrix and R(t) is |V| X 1 so DR(t) produces one value 
// which is equally added to all the nodes which it the dangling score for the job
public class DanglingDriver {
	
	// Global counter for maintaining Dangling Node contributions
	public static enum UpdateCounter{
		DANGLINGOFFSET;
	}
	
	
	// Method that run the IteratePageRank Job and returns the Job object for the ease of
	// access of Global Counters for that job anywhere function gets invoked from.
	public static Job runDanglingCalculation(String[] args, Configuration conf) throws Exception {

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Iteration Page Rank");
		job.setJarByClass(DanglingDriver.class);
		
		// Multiple Mapper configuration one for D vector and one for R
		MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, DanglingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, RankMapper.class);
        FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
        
		job.setReducerClass(DanglingReducer.class);
		job.setMapOutputKeyClass(MatrixElement.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		
		return job;

	}	

}
