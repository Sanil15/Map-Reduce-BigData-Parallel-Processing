package com.mapreduce.dangling;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Dangling Mapper just emits ((i,j,k),val) where
// It reads a compressed version of D which is 1 X |V| matrix
public class DanglingMapper extends Mapper<Object, Text, MatrixElement, DoubleWritable> {

	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] pageDetails = value.toString().split("\t");

		MatrixElement page = new MatrixElement();
		
		// i = 0 since D has only one row
		page.setI(new LongWritable(0L));
		// j = 0 since R has one column
		page.setJ(new LongWritable(0L));
		// k is pageId 
		page.setK(new LongWritable(Long.parseLong(pageDetails[0])));

		ctx.write(page, new DoubleWritable(Double.parseDouble(pageDetails[1])));
	}
}
