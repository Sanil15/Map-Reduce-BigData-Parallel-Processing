package com.threads.partb.coarse_lock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.threads.commons.ReadUtils;
import com.threads.commons.Station;

public class CalculateTMAXCoarseLock {

	// Common accumulating data structure i.e a map with a key as Station id and value as Station object
	public static HashMap<String,Station> stationMap = new HashMap<String,Station>(); 

	// Function spawns four threads each concurrently running on list of records given to it as 
	// argument and threads hold coarse lock i.e if one threads acquires access to common accumulating 
	// data structure it holds lock over the it hence coarse locking
	public void coraseLockCalculation(List<List<String>> choppedList) throws InterruptedException{
		Thread t1 = new Thread(new WorkerThread(choppedList.get(0)));
		Thread t2 = new Thread(new WorkerThread(choppedList.get(1)));
		Thread t3 = new Thread(new WorkerThread(choppedList.get(2)));
		Thread t4 = new Thread(new WorkerThread(choppedList.get(3)));

		t1.start();
		t2.start();
		t3.start();
		t4.start();

		t1.join();
		t2.join();
		t3.join();	
		t4.join();

		ReadUtils.calculateAverageTemprature(stationMap);
	}

	// Function looks up for presence of a station id in the map, if it is not present
	// It creates a new Station object and puts it in the map with Station id as key
	// Otherwise if Station Id exists it updates map's entry by accumulating temperature 
	// values and incrementing the record count for that station. While performing this 
	// the thread holds lock on the entire accumulating data structure i.e map
	public synchronized static void lookUpAndModify(String[] stationDetail){
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
