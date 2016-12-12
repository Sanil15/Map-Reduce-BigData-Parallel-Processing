package com.mapreduce.training_and_validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Validate {
	public static void test(String input, String output, String trainOutput) throws Exception{
		Configuration conf = new Configuration();

		String[] a = new String[2];
		a[0] = input;
		a[1] = output;
		String[] otherArgs = new GenericOptionsParser(conf, a).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		conf.set("models", trainOutput);
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Validate.class);
		job.setMapperClass(ValidationMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);

	}

}
