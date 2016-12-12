package com.mapreduce.driver;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import com.mapreduce.cleansing.CleansingMapper;
import com.mapreduce.cleansing.CleansingReducer;

public class Cleansing {

	
  public static void cleanData(String input, String output) throws Exception {
	  
	  String[] dir = new String[2];
	  dir[0] = input;
	  dir[1] = output;
	  
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, dir).getRemainingArgs();
    
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Cleansing.class);
    job.setMapperClass(CleansingMapper.class);
    job.setReducerClass(CleansingReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    
    FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));
    
	// Multiple Output Configuration for producing four files as explained
	MultipleOutputs.addNamedOutput(job, "TRAIN", TextOutputFormat.class, Text.class, Text.class);
	MultipleOutputs.addNamedOutput(job, "TEST", TextOutputFormat.class, Text.class, Text.class);
	
    job.waitForCompletion(true);
    
  }
  
  
  
  
}
