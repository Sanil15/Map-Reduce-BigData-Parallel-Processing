package com.mapreduce.pagerank.rowbycolumn;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.pagerank.columnbyrow.DotProductCombiner;
import com.mapreduce.pagerank.columnbyrow.DotProductReducer;

// PageRankDriverRowByColumn performs on basic multiplication of two matrices that is M[i,k] * R[k,j] to produce
// result let say X[i,j]. Here M[i,k] gets multipled with R[k,0] since r is |V| X 1
public class PageRankDriverRowByColumn {


	// Probablity of Random Jump
	public static final Double DAMPINGFACTOR = 0.15;


	// Method that run the IteratePageRank Job and returns the Job object for the ease of
	// access of Global Counters for that job anywhere function gets invoked from.
	public static Job runPageRank(String[] args,String cache, Configuration conf) throws Exception {

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		conf.set("cache", cache);
		Job job = Job.getInstance(conf, "Iteration Page Rank");
		job.setJarByClass(PageRankDriverRowByColumn.class);		
		job.setMapperClass(AdjacencyMatrixMapperRowByColumn.class);
		// It uses reducer & combiner on same principles as in column * row job 
		job.setCombinerClass(DotProductCombiner.class);
		job.setReducerClass(DotProductReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

		job.waitForCompletion(true);
		return job;

	}	

}