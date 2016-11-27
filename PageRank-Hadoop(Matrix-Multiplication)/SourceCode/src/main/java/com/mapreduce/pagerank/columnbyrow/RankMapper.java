package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// RankMapper reads the R matrix that is the OutputR-r-00000 produced by preprocessing (first itration) and 
// R(t-1) for any t iteration produced by page rank job. 
// It emits emit((row,matrixId),(0,value)) as (MatrixElement element, MatrixTuple tuple) for every
// element in the R vector, row is the pageId and matrixID is "R" 
public class RankMapper extends Mapper<Object, Text, MatrixElement, MatrixTuple>{
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String []pageDetails = value.toString().split("\t");
		
		// Form the Key with matrixId "R" and row as pageId
		MatrixElement element = new MatrixElement();
		LongWritable row = new LongWritable(Long.parseLong(pageDetails[0]));
		element.setMatrix(new Text("R"));
		element.setRowOrColumn(row);
		
		// R is always a |V| X 1 Matrix so column always is 0
		MatrixTuple tuple = new MatrixTuple();
		tuple.setRowOrColumnId(new LongWritable(0L));
		tuple.setValue(new DoubleWritable(Double.parseDouble(pageDetails[1])));
		
		ctx.write(element, tuple);
	}
}
