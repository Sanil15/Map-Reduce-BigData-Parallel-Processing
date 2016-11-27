package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// AdjacencyMatrixMapper reads the M matrix that is the OutputM-r-00000 produced by preprocessing and decompresses it.
// It partitions the sparse matrix by columns, it works on 1-Bucket Random works algorithm to compute final result.
// It emits ((row,matrixId),(outlink.column,outlink.val)) as (MatrixElement element, MatrixTuple tuple) for every
// element in the outlink adjacency list,here row is the pageId and matrixID is "M" 
public class AdjacencyMatrixMapper extends Mapper<Object, Text, MatrixElement, MatrixTuple> {
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String[] pageDetails = value.toString().split("\t");
		String pageId = pageDetails[0];
		String[] adjacencyList = pageDetails[1].split("~");
		
		// Key to be emitted with matrixId "M" and row as pageId
		MatrixElement element = new MatrixElement();
		element.setMatrix(new Text("M"));
		element.setRowOrColumn(new LongWritable(Long.parseLong(pageId)));

		for(String outlink : adjacencyList){
			String[] details = outlink.split(",");			
			String col = details[0].substring(1, details[0].length());
			String val = details[1].substring(0, details[1].length() - 1);
			
			// Parse outlink values as a MatrixTuple
			MatrixTuple tuple = new MatrixTuple();
			tuple.setRowOrColumnId(new LongWritable(Long.parseLong(col)));
			tuple.setValue(new DoubleWritable(Double.parseDouble(val)));
			
			ctx.write(element, tuple);
			
		}

	}
}
