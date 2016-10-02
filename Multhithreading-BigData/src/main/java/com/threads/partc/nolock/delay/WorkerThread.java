package com.threads.partc.nolock.delay;

import java.util.List;

import com.threads.commons.Fibonacci;
import com.threads.commons.Station;
import com.threads.partb.no_lock.CalculateTMAXNoLock;

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
	// Otherwise if Station Id exists it makes a delayed update to map's entry by accumulating temperature 
	// values and incrementing the record count for that station. While these operations
	// the thread holds no kind of lock	
	@Override
	public void run() {
		try{
		for(String a: records){
			String []temp = a.split(",");
			if(temp[2].equals("TMAX")){
				if(CalculateTMAXNoLockWithDelay.stationMap.containsKey(temp[0])){
					Fibonacci.calculateFibonacci(17);
					Station obj = CalculateTMAXNoLockWithDelay.stationMap.get(temp[0]);
					obj.setRecordCount(obj.getRecordCount()+1);
					obj.setTmaxTotal(obj.getTmaxTotal()+Double.parseDouble(temp[3]));
					CalculateTMAXNoLock.stationMap.put(temp[0],obj);
				}
				else{
					Station obj = new Station(temp[0],Double.parseDouble(temp[3]),1,0.0);
					CalculateTMAXNoLockWithDelay.stationMap.put(temp[0],obj);
				}		
			}
		}
		}catch(Exception e){}
		
	}
	
}
