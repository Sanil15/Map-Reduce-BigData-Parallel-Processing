package com.mapreduce.dangling;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Rank Mapper just emits ((i,j,k),val) where
//It reads a compressed version of R which is |V| X 1 matrix
public class RankMapper extends Mapper<Object, Text, MatrixElement, DoubleWritable>{

	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {

		String []pageDetails = value.toString().split("\t");

		MatrixElement page = new MatrixElement();
		// i = 0 since D has only one row
		page.setI(new LongWritable(0L));
		// j = 0 since R has onlu one column
		page.setJ(new LongWritable(0L));
		// k = pageId
		page.setK(new LongWritable(Long.parseLong(pageDetails[0])));

		ctx.write(page, new DoubleWritable(Double.parseDouble(pageDetails[1])));
	}
}
