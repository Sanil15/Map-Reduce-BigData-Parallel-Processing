package com.mapreduce.top100;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Top100Records MapReduce job uses in mapper combining (inspired from TopK Records program in lesson 2.13 of Module 5 Basic Algorithms) 
// to just emit top 100 records for each Map task i.e top 100 records for each input split.
// Sorts them in decreasing order of keys (page rank) sends them to 0th reducer since only one reducer her hence just emit Global top 100 page ranks.
public class Top100Records {

	  public static void fetchTop100FrequentRecords(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Top100Records.class);
	   
	    MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, Top100Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, NameMapper.class);
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
	    
	    
	    job.setReducerClass(Top100Reducer.class); 
	    // Just one reducer 
	    job.setNumReduceTasks(1);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(RankOrName.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   
	    job.waitForCompletion(true);
	  
	  }
	  
	
}
