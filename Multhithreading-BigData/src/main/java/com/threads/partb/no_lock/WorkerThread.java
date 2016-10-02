package com.threads.partb.no_lock;

import java.util.List;

import com.threads.commons.Station;

//Thread will get initialized by records that is the list of string
//It reads every record and looks Up and modify on the accumulating data structure
//by passing the station details
public class WorkerThread implements Runnable {

	private final List<String> records;
	
	WorkerThread(List<String> records){
		this.records = records;
	}
	
	// Method is a lookup of presence of a station id in the map, if it is not present
	// It creates a new Station object and puts it in the map with Station id as key
	// Otherwise if Station Id exists it updates map's entry by accumulating temperature 
	// values and incrementing the record count for that station. While these operations
	// the thread holds no kind of lock	
	@Override
	public void run() {
		try{
		for(String a: records){
			String []stationDetail = a.split(",");
			if(stationDetail[2].equals("TMAX")){
				String stationId = stationDetail[0];
				String temp = stationDetail[3];
				if(CalculateTMAXNoLock.stationMap.containsKey(stationId)){
					Station obj = CalculateTMAXNoLock.stationMap.get(stationId);
					obj.setRecordCount(obj.getRecordCount()+1);
					obj.setTmaxTotal(obj.getTmaxTotal()+Double.parseDouble(temp));
					CalculateTMAXNoLock.stationMap.put(stationId,obj);
				}
				else{
					Station obj = new Station(stationId,Double.parseDouble(temp),1,0.0);
					CalculateTMAXNoLock.stationMap.put(stationId,obj);
				}
			}
		}
		}catch(Exception e){}
	}
	
}
