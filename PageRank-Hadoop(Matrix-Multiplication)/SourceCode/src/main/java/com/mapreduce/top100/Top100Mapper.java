package com.mapreduce.top100;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Top100Mapper uses InMapper combining for just emitting top 100 (highest page ranks) records for a Map task
// with highest page rank values to the reducer.
public class Top100Mapper extends Mapper<Object, Text, LongWritable, RankOrName>{
	private static int emitCounter = 100;
	HashMap<Long,Double> topMap;
	
	public void setup(Context context){
		topMap = new HashMap<Long,Double>();
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pageDetails[] = value.toString().split("\t");
		String pageId = pageDetails[0].trim();
		String pageRank = pageDetails[1].trim();
		// Populate the Map with (pageId,pageRank)
		topMap.put(Long.parseLong(pageId), Double.parseDouble(pageRank));

	}
	
	public void cleanup(Context context){
		List<Entry<Long,Double>> sortedList = new ArrayList<Entry<Long,Double>>(topMap.entrySet());
		
		// Sort the topMap in decreasing order of values to reorder map with highest ranks in a sortedList
		Collections.sort(sortedList,new Comparator<Entry<Long,Double>>() {
			@Override
			public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
				// TODO Auto-generated method stub
				
				if(o2.getValue() > o1.getValue())
					return 1;
				
				else if(o2.getValue() < o1.getValue())
					return -1;
				
				else
					return 0;
			}
			
		});
		
		// Emit just top 100 records in the sorted list
		for(Entry<Long,Double> ent:sortedList){
			if(emitCounter > 0){
				try {
					RankOrName rankOrName = new RankOrName(new Text("Rank"), new Text(Double.toString(ent.getValue())));
					context.write(new LongWritable(ent.getKey()), rankOrName);
					emitCounter--;
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
				
		}
		
	}

}
