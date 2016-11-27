package com.mapreduce.pagerank.rowbycolumn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// AdjacencyMatrixMapperRowByColumn is a simple row * column multiplication happening for calculating M X R(t)
// M[k,i] X R[i,j], here j = 0. We first create an in-memory lookup for optimizing any R[k,0] lookup for all the
// elements in one row of M. them we multiply and aggregate sum of local dot product using in-mapper combining for
// efficiency.  
public class AdjacencyMatrixMapperRowByColumn extends Mapper<Object, Text, LongWritable, DoubleWritable> {

	// Will read R(t) file in memory for fast access to create R(t+1)
	private HashMap<Long,Double> rankMap;
	// Used for InMapper Combining for a pageId it maintains sum of all its dot product for row * column partition
	private HashMap<Long,Double> aggregateMap;


	protected void setup(Context context) throws IOException, InterruptedException {
		rankMap = new HashMap<Long,Double>();
		aggregateMap = new HashMap<Long,Double>();

		// Read from HDFS to create a [pageId,pageRank] lookup called rankMap 
		try{
			String cachePath = context.getConfiguration().get("cache");
			Path pt=new Path(cachePath);
			FileSystem fs = FileSystem.get(URI.create(cachePath),context.getConfiguration());
			FileStatus[] status = fs.listStatus(pt);
			for (int i=0;i<status.length;i++){
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				line=br.readLine();
				while (line != null){
					if(line !=null && line.trim().length() > 0){
					String[] details = line.split("\t");
					rankMap.put(new Long(Long.parseLong(details[0].trim())), new Double(Double.parseDouble(details[1].trim())));
					}
					line=br.readLine();
				}
			
			}

		} catch(Exception ex) {
			System.err.println("Exception in mapper setup:" + ex.getMessage());
		}

	}


	// Mapper follows In-Mapper Combining design pattern for efficiency
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] pageDetails = value.toString().split("\t");
		String pageId = pageDetails[0];
		String[] adjacencyList = pageDetails[1].split("~");

		// calculate M[k,i] X R[i,0] for every i and k = pageId where i is outlink
		for(String outlink : adjacencyList){
			String[] details = outlink.split(",");			
			String row = details[0].substring(1, details[0].length());
			String val = details[1].substring(0, details[1].length() - 1);
			Double dotProduct = 0.0d;
			
			dotProduct = Double.parseDouble(val) * rankMap.get(Long.parseLong(pageId));
			// Aggregate all the values in a local Map for in-mapper combining 
			if(aggregateMap.containsKey(Long.parseLong(row)))
				aggregateMap.put(Long.parseLong(row), aggregateMap.get(Long.parseLong(row)) + dotProduct);
			else
				aggregateMap.put(Long.parseLong(row),dotProduct);

		}

		// Emit the Node itself 
		ctx.write(new LongWritable(Long.parseLong(pageId)), new DoubleWritable(0.0d));

	}

	// Emit all pageId and sum of local contributions 
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(Map.Entry<Long, Double> entry: aggregateMap.entrySet())
			context.write(new LongWritable(entry.getKey()), new DoubleWritable(entry.getValue()));
	}
}

