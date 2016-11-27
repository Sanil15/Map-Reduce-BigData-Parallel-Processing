package com.mapreduce.parser;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.mapreduce.parser.Bz2WikiParserMR.UpdateCounter;

// Bz2Reducer maintains two collections: one HashMap(pageName,pageId) for all nodes in graph called pageNameMapping and a set for dangling node ids.
// It first read all the values checks whether key as well as values exist in the pageNameMapping, if yes it fetches there pageId otherwise it assigns a new id to them
// by updating a counter. It writes four files, one for non-dangling nodes, one for dangling nodes, one for pageName and pageId mapping and last one for vector R(0) that
// represents nodeId and 1/|V| for initial vector R(0)
public class Bz2Reducer extends Reducer<Text,Text,Text,Text> {

	// Configure Multiple Outputs
	private MultipleOutputs mos;
	// LookUp for PageName and PageId
	private HashMap<String,Long> pageNameMapping;
	// Set storing PageName for dangling nodes
	private HashSet<String> danglingNodeSet;

	public void setup(Context context){
		mos = new MultipleOutputs<Text,Text>(context);
		pageNameMapping = new HashMap<String,Long>();
		danglingNodeSet = new HashSet<String>();
	}

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

		Text pageName = key;
		ArrayList<String> adList = new ArrayList<String>();
		// Recover the adjacency list
		for(Text obj: values){
			if(!obj.equals("") && obj.toString().length() > 1)
				adList.add(obj.toString());
		}

		StringBuilder sb = new StringBuilder();

		// Check if PageName already exists in pageNameMapping, if yes 
		// fetch its pageId
		if(pageNameMapping.containsKey(pageName.toString())){
			sb.append(pageNameMapping.get(pageName.toString()));
			sb.append("\t");
		}
		// Otherwise allocate a new id to page by using a global counter and 
		// add it to the pageNameMapping
		else{
			Long id = context.getCounter(UpdateCounter.PAGENAMECOUNT).getValue();
			context.getCounter(UpdateCounter.PAGENAMECOUNT).increment((1));
			pageNameMapping.put(key.toString(), id);
			sb.append(pageNameMapping.get(pageName.toString()));
			sb.append("\t");
		}

		// If the node is not dangling then create a string with format
		// PageId (OutlinkId1,1/adjacencyList.length)~(OutlinkId2,1/adjacencyList.length)~(OutlinkId3,1/adjacencyList.length)...
		// Check if valid outlink pageId exist, if they dont create unique id for them. 
		// Append this result string to "OutputM" file
		if(adList.size() > 0){
			for(String outlink: adList){
				outlink = outlink.trim();
				if(outlink.length() > 0){
					Long k = -1L;
					if(pageNameMapping.containsKey(outlink)){
						k = pageNameMapping.get(outlink);
					}
					else{
						Long id = context.getCounter(UpdateCounter.PAGENAMECOUNT).getValue();
						k = id;
						context.getCounter(UpdateCounter.PAGENAMECOUNT).increment((1));
						pageNameMapping.put(outlink, id);
					}
					Double initialValue = 1.0 / adList.size();
					sb.append("("+k+","+initialValue+")~");
				}
			}
			sb.setLength(sb.length() - 1);
			// Append the string to OutputM file
			mos.write("OutputM", new Text(sb.toString()), new Text(""));
		}
		// Just add this node to dangling node set
		else{
			danglingNodeSet.add(key.toString());
		}


	}

	public void cleanup(Context context) throws IOException, InterruptedException{

		// Iterate over the pageNameMapping HashMap to create vector R(0) in OutputR file
		// and Create a file called OutputN that contains mapping for PageName,PageId
		for(Map.Entry<String, Long> entrySet: pageNameMapping.entrySet()){
			String pageName = entrySet.getKey();
			// Append all the dangling node Ids to a file called OutputD with initial value as 1/|V|
			if(danglingNodeSet.contains(pageName)){
				Double initalValue = 1.0 / context.getCounter(UpdateCounter.PAGENAMECOUNT).getValue();
				Long pageId = pageNameMapping.get(pageName);
				mos.write("OutputD", new Text(pageId.toString()), new Text(initalValue.toString()));
			}	
			Long pageId = entrySet.getValue();
			Double initalValue = 1.0 / context.getCounter(UpdateCounter.PAGENAMECOUNT).getValue();
			mos.write("OutputR", new Text(pageId.toString()) , new Text(initalValue.toString()));
			mos.write("OutputN", new Text(pageName), new Text(pageId.toString()));
		}
	
		mos.close();
	}
}
