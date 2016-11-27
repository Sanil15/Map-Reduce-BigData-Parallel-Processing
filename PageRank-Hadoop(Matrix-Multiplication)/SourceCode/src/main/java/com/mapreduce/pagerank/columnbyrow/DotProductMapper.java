package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Reads the intermediate DUMR(t) file produced by the PageRankDriver job, uses in-mapper combining to 
// perform local aggregation for Σ p(m)/c(m) for any pageId.
public class DotProductMapper extends Mapper<Object, Text, LongWritable, DoubleWritable> {

	// lookup for in-mapper combining
	private HashMap<Long,Double> aggregateMap;
	
	public void setup(Context ctx){
		aggregateMap = new HashMap<Long,Double>();
	}
	
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
			
		String[] pageDetails = value.toString().split("\t");
		String pageId = pageDetails[0];
		String contribution = pageDetails[1];
		
		// Initialize or increment sum of contributions in the local aggregateMap
		if(aggregateMap.containsKey(Long.parseLong(pageId)))
			aggregateMap.put(Long.parseLong(pageId), Double.parseDouble(contribution) + aggregateMap.get(Long.parseLong(pageId)));
		else
			aggregateMap.put(Long.parseLong(pageId), Double.parseDouble(contribution));
		
	}
	
	// Emit pageId/row and local aggregate of contributions 
	public void cleanup(Context ctx) throws IOException, InterruptedException{
		for(Map.Entry<Long,Double> entry: aggregateMap.entrySet()){
			ctx.write(new LongWritable(entry.getKey()), new DoubleWritable(entry.getValue()));
		}
	}
}
