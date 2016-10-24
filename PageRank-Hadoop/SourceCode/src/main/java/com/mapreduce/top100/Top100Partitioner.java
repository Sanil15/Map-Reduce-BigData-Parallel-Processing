package com.mapreduce.top100;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Custom partitioner which sends all the records to just a single reducer (0th)  
public class Top100Partitioner extends Partitioner<DoubleWritable, Text>{
	@Override
	public int getPartition(DoubleWritable arg0, Text arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}
	  
}
