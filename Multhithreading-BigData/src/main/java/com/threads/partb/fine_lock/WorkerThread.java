package com.threads.partb.fine_lock;

import java.util.List;
import com.threads.commons.Station;

//Thread will get initialized by records that is the list of string
//It reads every record, it looks up and modify the accumulating data structure
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
	// The thread holds a lock on Station Object
	@Override
	public void run() {
		for(String a: records){
			String []stationDetail = a.split(",");
			if(stationDetail[2].equals("TMAX")){
				
				// If Station id already exists in station map, hold a lock and update the existing map entry
				if(CalculateTMAXFineLock.stationMap.containsKey(stationDetail[0])){
					Station obj = CalculateTMAXFineLock.stationMap.get(stationDetail[0]);
					// a Synchronized method in Station Object
					obj.setAttributes(obj.getTmaxTotal()+Double.parseDouble(stationDetail[3]),obj.getRecordCount()+1);
					CalculateTMAXFineLock.stationMap.put(stationDetail[0],obj);
				}
				else{
					Station objNew = new Station(stationDetail[0],Double.parseDouble(stationDetail[3]),1,0.0);
					// Hold the lock for the Station Object that does not exists
					synchronized (objNew) {
						// For Fine Lock if two threads want to add same element with same Station Key
						// we still need a check that has any of those thread who acquired lock first has
						// added the Station to station map, otherwise update the station while holding a lock
						if(!CalculateTMAXFineLock.stationMap.containsKey(stationDetail[0]))
						CalculateTMAXFineLock.stationMap.put(stationDetail[0],objNew);	
						
						else{
							Station objExisting = CalculateTMAXFineLock.stationMap.get(stationDetail[0]);
							objExisting.setAttributes(objExisting.getTmaxTotal()+objNew.getTmaxTotal(), objExisting.getRecordCount()+1);
							CalculateTMAXFineLock.stationMap.put(stationDetail[0],objExisting);
						}
							
					}
				}
			}
		}
		

	}
	
}
