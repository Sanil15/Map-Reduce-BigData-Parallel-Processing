package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.omg.CORBA.CTX_RESTRICT_SCOPE;

import com.mapreduce.dangling.MatrixElement;

// Add the contributions for the same keys
public class DotProductCombiner extends Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable>{
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,Context ctx) throws IOException, InterruptedException {
	Double sum = 0.0d;	
		
	for(DoubleWritable products : values){
		sum = sum + products.get();
	}
		ctx.write(key,new DoubleWritable(sum));
	}
}
