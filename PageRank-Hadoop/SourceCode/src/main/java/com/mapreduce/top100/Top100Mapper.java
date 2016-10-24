package com.mapreduce.top100;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Top100Mapper uses InMapper combining for just emitting top 100 records for a Map task
// with highest page rank values to the reducer.
public class Top100Mapper extends Mapper<Object, Text, DoubleWritable, Text>{
	private static int emitCounter = 100;
	HashMap<Text,DoubleWritable> topMap;
	
	public void setup(Context context){
		topMap = new HashMap<Text,DoubleWritable>();
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pageDetails[] = value.toString().split("\t");
		String pageName = pageDetails[0].trim();
		String pageRank = pageDetails[1].trim();
		topMap.put(new Text(pageName), new DoubleWritable(Double.parseDouble(pageRank)));

	}
	
	public void cleanup(Context context){
		List<Entry<Text,DoubleWritable>> sortedList = new ArrayList<Entry<Text,DoubleWritable>>(topMap.entrySet());
		
		// Sort the topMap in decreasing order of keys to reorder map with highest keys in a sortedList
		Collections.sort(sortedList,new Comparator<Entry<Text,DoubleWritable>>() {
			@Override
			public int compare(Entry<Text, DoubleWritable> o1, Entry<Text, DoubleWritable> o2) {
				// TODO Auto-generated method stub
				
				if(o2.getValue().get() > o1.getValue().get())
					return 1;
				
				else if(o2.getValue().get() < o1.getValue().get())
					return -1;
				
				else
					return 0;
			}
			
		});
		
		// Emit just top 100 records in the sorted list
		for(Entry<Text,DoubleWritable> ent:sortedList){
			if(emitCounter > 0){
				try {
					context.write(ent.getValue(), ent.getKey());
					emitCounter--;
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
				
		}
		
	}

}
