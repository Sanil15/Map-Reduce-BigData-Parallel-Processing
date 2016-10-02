package com.threads.partb.coarse_lock;

import java.util.List;

// Thread will get initialized by records that is the list of string
// It reads every record and invokes lookUpAndModify on the accumulating data structure
// by passing the station details while holding over accumulating data structure
public class WorkerThread implements Runnable {

	private final List<String> records;
	
	WorkerThread(List<String> records){
		this.records = records;
	}
		
	@Override
	public void run() {
		for(String a: records){
			String []stationDetail = a.split(",");
			CalculateTMAXCoarseLock.lookUpAndModify(stationDetail);
		}
		
	}
	
}
