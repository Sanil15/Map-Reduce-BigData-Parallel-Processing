package com.mapreduce.training_and_validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Train {

	public static void runLoader(String input, String output) throws Exception{
		
		Configuration conf = new Configuration();

		String[] a = new String[2];
		a[0] = input;
		a[1] = output;
		String[] otherArgs = new GenericOptionsParser(conf, a).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Train.class);
		job.setMapperClass(TrainMapper.class);
		job.setReducerClass(TrainReducer.class);
		job.setPartitionerClass(TrainPartitioner.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);


	}

}
