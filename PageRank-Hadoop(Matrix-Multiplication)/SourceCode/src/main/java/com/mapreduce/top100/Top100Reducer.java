package com.mapreduce.top100;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// Top100Reducer just emits the first 100 records that it reads
public class Top100Reducer extends Reducer<LongWritable,RankOrName,Text,Text> {
	
	private static HashMap<String,Double> totalMap;
		
	
	public void setup(Context context){
		totalMap = new HashMap<String,Double>();
	}
	  
    public void reduce(LongWritable key, Iterable<RankOrName> values, Context context) throws IOException, InterruptedException {
    
    	java.util.Iterator<RankOrName> v = values.iterator();
    	String pageName = null;
    	Double pageRank = -1.0d;
    	
    	while(v.hasNext()){
    		RankOrName rankOrName = (RankOrName) v.next();
    		
    		if(rankOrName.getType().toString().equals("Rank"))
    			pageRank = Double.parseDouble(rankOrName.getValue().toString());
    		else
    			pageName = rankOrName.getValue().toString();
    	}
    	totalMap.put(pageName, pageRank);
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException{
    	
    	List<Map.Entry<String,Double>> sortedTotal = new ArrayList<Map.Entry<String,Double>>(totalMap.entrySet());
    	Collections.sort(sortedTotal, new Comparator<Map.Entry<String, Double>>() {

			@Override
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
    	});
    	
    	
    	for(Map.Entry<String, Double> page: sortedTotal.subList(0, 100)){
    		context.write(new Text(page.getKey()), new Text(Double.toString(page.getValue())));
    	}
    	
    }
  }
