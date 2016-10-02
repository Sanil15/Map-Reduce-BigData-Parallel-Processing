package com.threads.partb.no_sharing;

import java.util.HashMap;
import java.util.List;
import com.threads.commons.Station;

//Thread will get initialized by records that is the list of string
//It reads every record and it looks Up and modify its private accumulating data structure
//by passing the station details
public class WorkerThread extends Thread {

	// Private accumulating data structure for every Thread
	// i.e a map with Key as Station id and value as Station Id 
	private HashMap<String,Station> stationMap; 
	
	private final List<String> records;
	
	WorkerThread(List<String> records){
		this.records = records;
		stationMap = new HashMap<String,Station>();
	}
	
	public HashMap<String,Station> getMap(){
		return stationMap;
	}
		
	// Method is a lookup for presence of a station id in the map, if it is not present
	// It creates a new Station object and puts it in the private map with Station id as key
	// Otherwise if Station Id exists it updates map's entry by accumulating temperature 
	// values and incrementing the record count for that station.
	@Override
	public void run() {
		
		for(String a: records){
			String []stationDetail = a.split(",");
			if(stationDetail[2].equals("TMAX")){
				if(stationMap.containsKey(stationDetail[0])){
					Station obj = stationMap.get(stationDetail[0]);
					obj.setRecordCount(obj.getRecordCount()+1);
					obj.setTmaxTotal(obj.getTmaxTotal()+Double.parseDouble(stationDetail[3]));
					stationMap.put(stationDetail[0],obj);
				}
				else{
					Station obj = new Station(stationDetail[0],Double.parseDouble(stationDetail[3]),1,0.0);
					stationMap.put(stationDetail[0],obj);
				}		
			}
		}
		
		
		
	}
	
}
