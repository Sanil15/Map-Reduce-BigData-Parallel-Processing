package com.mapreduce.pagerank.columnbyrow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.pagerank.columnbyrow.PageRankDriver.GroupComparator;
import com.mapreduce.pagerank.columnbyrow.PageRankDriver.KeyComparator;
import com.mapreduce.pagerank.columnbyrow.PageRankDriver.RangePartitioner;

// Part-1 Job that is the page rank job produces dot product for the all possible pages in a DUMMY files 
// This job just aggregates or just performs a reduce by key for all elements with same key.
// It calculates sum of all dot products received for one page and then calculates its new page rank value and 
// creates a vector R(t) i.e pageId,pageRank for t iteration. 
public class DotProductDriver {

	// Probablity of Random Jump
		public static final Double DAMPINGFACTOR = 0.15;
			
		
		public static Job runDotProduct(String[] args, Configuration conf) throws Exception {

			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length < 2) {
				System.err.println("Usage: wordcount <in> [<in>...] <out>");
				System.exit(2);
			}

			Job job = Job.getInstance(conf, "Iteration Page Rank");
			job.setJarByClass(DotProductDriver.class);
			job.setMapperClass(DotProductMapper.class);
			job.setCombinerClass(DotProductCombiner.class);
			job.setReducerClass(DotProductReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			for (int i = 0; i < otherArgs.length - 1; ++i) {
				FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
			}
			FileOutputFormat.setOutputPath(job,
					new Path(otherArgs[otherArgs.length - 1]));

			job.waitForCompletion(true);
			
			return job;

		}	


	
}
