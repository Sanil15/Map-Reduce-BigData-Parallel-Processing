package com.threads.partc.nosharing.delay;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.threads.commons.ReadUtils;
import com.threads.commons.Station;

public class CalculateTMAXNoSharingWithDelay {
	
	// Function spawns four threads each concurrently running on list of records given to it as 
	// argument and thread run concurrently with no sharing mechanism
	public void noSharingCalculation(List<List<String>> choppedList) throws InterruptedException{
		WorkerThread t1 = new WorkerThread(choppedList.get(0));
		WorkerThread t2 = new WorkerThread(choppedList.get(1));
		WorkerThread t3 = new WorkerThread(choppedList.get(2));
		WorkerThread t4 = new WorkerThread(choppedList.get(3));
		
		t1.start();
		t2.start();
		t3.start();
		t4.start();

		t1.join();
		t2.join();
		t3.join();	
		t4.join();
		
		HashMap<String,Station> ans = new HashMap<String,Station>();
		mergeMap(t1.getMap(),ans);
		mergeMap(t2.getMap(),ans);
		mergeMap(t3.getMap(),ans);
		mergeMap(t4.getMap(),ans);
			
		ReadUtils.calculateAverageTemprature(ans);
		
	}
	
	// Function takes two arguments source HashMap and destination HashMap with key as Station Id and
	// Value as Station object, it merges destination map with source map and accumulates objects with 
	// same station id by combining their cumulative attributes
	public static void mergeMap(HashMap<String,Station> source, HashMap<String,Station> destination){
		for(Map.Entry<String,Station> obj : source.entrySet()){
			if(destination.containsKey(obj.getKey())){
				String stationId = obj.getKey();
				Station sourceTemp = obj.getValue();
				Station destinationTemp = destination.get(stationId);
				Station temp = new Station(stationId,sourceTemp.getTmaxTotal()+destinationTemp.getTmaxTotal(),sourceTemp.getRecordCount()+destinationTemp.getRecordCount(),0.0);
				destination.put(stationId,temp);		
			}
			else{
				String stationId = obj.getKey();
				Station sourceTemp = obj.getValue();
				Station temp = new Station(stationId,sourceTemp.getTmaxTotal(),sourceTemp.getRecordCount(),0.0);
				destination.put(stationId,temp);
			}
		}
		
	}
	
}
