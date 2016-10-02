package com.threads.partc.finelock.delay;

import java.util.HashMap;
import java.util.List;
import com.threads.commons.ReadUtils;
import com.threads.commons.Station;

public class CalculateTMAXFineLockWithDelay {

	// Common accumulating data structure i.e a map with Key as Station id and value as Station Id
	public static HashMap<String,Station> stationMap = new HashMap<String,Station>();
	
	// Function spawns four threads each concurrently running on list of records given to it as 
	// argument and threads hold fine lock i.e if one threads acquires access to Station Object it holds
	// lock over the Station Object hence fine locking
	public void fineCalculation(List<List<String>> choppedList) throws InterruptedException{
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
	
	
}
