package com.mapreduce.pagerank;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.mapreduce.pagerank.IteratePageRank.UpdateCounter;
import com.mapreduce.pagerank.IteratePageRank.UpdatePageNameCounter;

// IteratePageRank reducer gathers all the pages with same page names
// Checks for there dangling contributions (if any), then adds their normal
// contribution from other pages (inlinks) and updates ther respective page ranks 
// and the Global counter for Dangling offset
public class IteratePageRankReducer extends Reducer<Text,Node,Text,Node> {

	public void reduce(Text key, Iterable<Node> values,Context ctx) throws IOException, InterruptedException {
		// Maintains sum of contributions of page ranks from different pages to the current page (from inlinks)
		Double contribution = 0.0;
		// Maintains dangling contribution of current page to add to Global counter
		Double danglingContribution = 0.0;
		
		Node node = new Node();	
		node.setPageName(key);

		
		for(Node temp: values){				
			// Recover the graph structure
			if(temp.getAdjacencyList().size() != 0){
				node.setAdjacencyList(temp.getAdjacencyList());
			}
			// Maintain the dangling contribution if identified
			else if(temp.getPageName().toString().equals(IteratePageRank.DANGLINGNODEIDENTIFIER.toString())){
				DoubleWritable c = temp.getPageRank();
				danglingContribution += c.get();
			}
			// Otherwise just maintain running sum of contribution from the other pages (from inlinks) 
			else{
				DoubleWritable c = temp.getPageRank();
				contribution += c.get();
			}
			
		}

		// Calculate the new page rank with the formula 
		node.setPageRank(new DoubleWritable( (double)(IteratePageRank.DAMPINGFACTOR/Double.parseDouble(ctx.getConfiguration().get("pageNameCount")))
				+((1-IteratePageRank.DAMPINGFACTOR) * (contribution + Double.parseDouble(ctx.getConfiguration().get("danglingOffset"))))));
		
		// Update global dangling offset, if any dangling contributions made 
		if(danglingContribution > -1){
			long test = (long) (danglingContribution * 1000000000); //convert to long
			ctx.getCounter(UpdateCounter.DANGLINGOFFSET).increment((test));
		}
		
		
		// Update the page name count in global counter
		ctx.getCounter(UpdatePageNameCounter.PAGENAMECOUNTER).increment(1);
		ctx.write(key, node);
		
	}
}