package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// DotProductReducer produces R vector for the next page rank iteration.  
public class DotProductReducer extends Reducer<LongWritable,DoubleWritable,Text,Text>{

	private Long pageCount;
	private Long danglingLong;
	
	public void setup(Context ctx){
		pageCount = ctx.getConfiguration().getLong("pageNameCount", 0L);
		danglingLong = ctx.getConfiguration().getLong("danglingOffset", 0L);
	}
	
	public void reduce(LongWritable key, Iterable<DoubleWritable> values,Context ctx) throws IOException, InterruptedException {
		
		Double sum = 0.0d;	
		
		for(DoubleWritable products : values){
			sum = sum + products.get();
		}
	
		// δ / |V| = dangling contribution for the job
		Double danglingPR = (double)danglingLong / Math.pow(10, 16);
		// Pagerank computation: α / |V| + (1-α)( δ / |V|+ Σ p(m)/c(m));
		Double pageRankValue = ((PageRankDriver.DAMPINGFACTOR / pageCount) + ((1-PageRankDriver.DAMPINGFACTOR)*(sum + danglingPR)));
		
		// Emitting pageId and pageRank to produce R vector 
		ctx.write(new Text(Long.toString(key.get())), new Text(pageRankValue.toString()));

	}
}
