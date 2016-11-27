package com.mapreduce.pagerank.columnbyrow;


import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Computation of 𝐌′𝐑(𝑡) = (𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡) 
// This contributes to first half of 𝐌𝐑(𝑡). This job is configured to use Two different
// Mappers i.e one for reading sparse matrix M from file OutputM-r-00000
// other Mapper to read either initail R(0) vector from OutputR-r-00000 for the first iteration
// otherwise R(t-1) vector from previous job for calculating R(t)
// The algorithm performs equi-join of any M[x,i] with any R[y,0] using condition x = y
// Corresponding product is emitted with the key (i,0) and stored i only since column is always 0
public class PageRankDriver {
	
	// Probablity of Random Jump
	public static final Double DAMPINGFACTOR = 0.15;
		
	public static Job runPageRank(String[] args, Configuration conf) throws Exception {

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Iteration Page Rank");
		job.setJarByClass(PageRankDriver.class);

		// Configuration of Multiple Mappers
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, AdjacencyMatrixMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, RankMapper.class);
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));

		job.setReducerClass(PageRankReducer.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(RangePartitioner.class);
		job.setMapOutputKeyClass(MatrixElement.class);
		job.setMapOutputValueClass(MatrixTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		
		return job;

	}	

	// Ensures Composite Keys with Same row go to same reducer task
	public static class RangePartitioner extends Partitioner<MatrixElement, MatrixTuple>{
		@Override
		public int getPartition(MatrixElement key, MatrixTuple val, int numReds) {
			// TODO Auto-generated method stub
			return key.getRowOrColumn().hashCode() % numReds;
		}
	}

	// Sorts in increasing order of rows in the composite key
	// If row equal it ensures that key with matrixId "R" ends 
	// up in reducer first as compared to matrixId "M"
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(MatrixElement.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			MatrixElement o1 = (MatrixElement) w1;
			MatrixElement o2 = (MatrixElement) w2;

			Long row1 = o1.getRowOrColumn().get();
			Long row2 = o2.getRowOrColumn().get();
			
			if(row1.compareTo(row2) == 0){
				return o2.getMatrix().toString().compareTo(o1.getMatrix().toString());
			}
			return row1.compareTo(row2);

		}

	}

	// Sorts in increasing order of rows in composite key
	// Ensures records with same row value in composite key end up in same reduce call
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(MatrixElement.class,true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			MatrixElement st1 = (MatrixElement) w1;
			MatrixElement st2 = (MatrixElement) w2;
			Long i1 = st1.getRowOrColumn().get();
			Long i2 = st2.getRowOrColumn().get();
	
			return i1.compareTo(i2);
		}

	}

}
