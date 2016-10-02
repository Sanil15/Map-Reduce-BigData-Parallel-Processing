package com.threads.partb.sequential;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.threads.commons.ReadUtils;
import com.threads.commons.Station;

public class CalculateTMAXSequential {

	// For Sequential Lock Calculation, Accumulating data structure i.e the HashMap 
	// will be shared by all the members of the call,, key of the map is station id, 
	// and value is Station object
	public static HashMap<String,Station> stationMap = new HashMap<String,Station>();; 
	
	// Function calculates sequentially the average of TMAX temperatures for all records
	// It takes an argument of records and parses its attributes as a Station object 
	// It looks up for the presence of Station in the accumulating data structure, if it exists
	// it accumulates the attributes in already present object with a delay, else puts a new element in map
	public void sequentialCalculation(List<String> records){
				
		for(String a: records){
			String []temp = a.split(",");
			if(temp[2].equals("TMAX")){
				if(stationMap.containsKey(temp[0])){
					Station obj = stationMap.get(temp[0]);
					obj.setRecordCount(obj.getRecordCount()+1);
					obj.setTmaxTotal(obj.getTmaxTotal()+Double.parseDouble(temp[3]));
					stationMap.put(temp[0],obj);
				}
				else{
					Station obj = new Station(temp[0],Double.parseDouble(temp[3]),1,0.0);
					stationMap.put(temp[0],obj);
				}		
			}
		}
		
		ReadUtils.calculateAverageTemprature(stationMap);

	}
	
	
}
