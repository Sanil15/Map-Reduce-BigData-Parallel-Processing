package com.mapreduce.top100;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Top100Reducer just emits the first 100 records that it reads
public class Top100Reducer extends Reducer<DoubleWritable,Text,Text,Text> {
	
	public static Integer count = 100;		
	  
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
    	for(Text pageName : values){
    		if(count > 0){
    			context.write(pageName, new Text(Double.toString(key.get())));
    			count--;
    		}
    	}
    	
    }
  }
