package com.mapreduce.training_and_validation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** TrainPartitioner determines the reducer based on the bag-id
 * */

public class TrainPartitioner extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text arg1, int arg2) {
		return key.get();
	}
}