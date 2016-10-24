package com.mapreduce.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// IteratePageRank mapper reads file line by file, parses the data as Node object
// Check whether it is a dangling node (if yes adds a DANGLINGNODEIDENTIFIER to it)
// splits and emits the current page rank of page equally across all its outlinks
public class IteratePageRankMapper extends Mapper<Object, Text, Text, Node>{
	
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {

		// Parse page attributes
		String pageDetails[] = value.toString().split("\t");
		String pageName = pageDetails[0].trim();
		String pageRank = pageDetails[1].trim();
		String adjacencyListStr = pageDetails[2];
	    adjacencyListStr = adjacencyListStr.substring(1, adjacencyListStr.length() - 1);
		String[] adjacencyList = adjacencyListStr.split(","); 

		// Add all the parsed attributes to a Node object
		Node currNode = new Node();
		currNode.setPageName(new Text(pageName));
		ArrayList<Text> list = new ArrayList<Text>();
		for(String outlink: adjacencyList){
			outlink = outlink.trim();
			if(outlink.length() > 0)
			list.add(new Text(outlink));			
		}
		currNode.setAdjacencyList(list);	
		double initialPageRank = Double.parseDouble(pageRank);

		
		// If the page rank is not set (has a garbage value), initialize page Rank with 1/N
		// where N is total number of page names  
		if(initialPageRank < 0){
			initialPageRank =(double) 1.0/Double.parseDouble(ctx.getConfiguration().get("pageNameCount"));
		}

		currNode.setPageRank(new DoubleWritable(initialPageRank));

		// Pass along the graph structure i.e Node object
		// If it is dangling node (has no outgoing links)
		// add a dangling node identifier to the object, otherwise don't
		if(currNode.getAdjacencyList().size() == 0){
			Text keyPageName = currNode.getPageName();
			currNode.setPageName(IteratePageRank.DANGLINGNODEIDENTIFIER);
			ctx.write(keyPageName, currNode);
		}
		else	
		ctx.write(currNode.getPageName(),currNode);


		// Compute contributions to send along outgoing links
		// i.e distribute current page rank across all outgoing links (if they exist)
		if(currNode.getAdjacencyList().size() > 0){
			DoubleWritable p = new DoubleWritable();
			p = new DoubleWritable((double)initialPageRank/adjacencyList.length);
			for(Text outlink:currNode.getAdjacencyList()){
				Node temp = new Node();
				temp.setPageRank(p);
				ctx.write(outlink,temp);	
			}
		}

	}
}
