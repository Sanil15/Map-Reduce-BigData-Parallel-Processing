package com.threads.main;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.threads.commons.Station;
import com.threads.partb.coarse_lock.CalculateTMAXCoarseLock;
import com.threads.partb.fine_lock.CalculateTMAXFineLock;
import com.threads.partb.no_lock.CalculateTMAXNoLock;
import com.threads.partb.no_sharing.CalculateTMAXNoSharing;
import com.threads.partb.sequential.CalculateTMAXSequential;
import com.threads.partc.coarselock.delay.CalculateTMAXCoarseLockWithDelay;
import com.threads.partc.finelock.delay.CalculateTMAXFineLockWithDelay;
import com.threads.partc.nolock.delay.CalculateTMAXNoLockWithDelay;
import com.threads.partc.nosharing.delay.CalculateTMAXNoSharingWithDelay;
import com.threads.partc.sequential.delay.CalculateTMAXSequentialWithDelay;

// This is Factory Class which has all the methods for various kind of Program execution
// Every Function invokes type of calculation as number of times as specified by parameter 
// iteration, collects all that data for execution time finds min, max and average of the data collected
public class RunFactory {

	// Function invokes sequential execution of program on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  
	public static void sequential(List<String> recordList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
		      	CalculateTMAXSequential obj = new CalculateTMAXSequential();
		      	CalculateTMAXSequential.stationMap = new HashMap<String,Station>();
		      	obj.sequentialCalculation(recordList);
		        Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Sequential Execution");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with no locks on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void noLock(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXNoLock objNoLock= new CalculateTMAXNoLock();
				CalculateTMAXNoLock.stationMap = new HashMap<String,Station>();
				try {
					objNoLock.noLockCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}    
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("No Lock Execution");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with fine locks on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void fineLock(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXFineLock objFineLock= new CalculateTMAXFineLock();
				CalculateTMAXFineLock.stationMap = new HashMap<String,Station>();
				try {
					objFineLock.fineCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Fine Lock Execution");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with coarse lock on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void coarseLock(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXCoarseLock objCoarseLock= new CalculateTMAXCoarseLock();
				CalculateTMAXCoarseLock.stationMap = new HashMap<String,Station>();
				try {
					objCoarseLock.coraseLockCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Coarse Lock Execution");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with no sharing mechanism on recordList as number of times as specfied by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void noSharingLock(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXNoSharing objNoSharing= new CalculateTMAXNoSharing();
				try {
					objNoSharing.noSharingCalculation(choppedList);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("No Sharing Execution");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes sequential execution of program with a delay on updating, on recordList as number  
	// of times as specified by iteration value, collects all data and finds min, max and average of collected data  	
	public static void sequentialWithDelay(List<String> recordList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
		      	CalculateTMAXSequentialWithDelay obj = new CalculateTMAXSequentialWithDelay();
		      	CalculateTMAXSequentialWithDelay.stationMap = new HashMap<String,Station>();
		      	obj.sequentialCalculation(recordList);
		        Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Sequential Execution With Delay");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with no locks and a delay on updating.It executes program number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data 
	public static void noLockWithDelay(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXNoLockWithDelay objNoLock= new CalculateTMAXNoLockWithDelay();
				CalculateTMAXNoLockWithDelay.stationMap = new HashMap<String,Station>();
				try {
					objNoLock.noLockCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}    
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("No Lock Execution With Delay");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with fine locks on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void fineLockWithDelay(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXFineLockWithDelay objFineLock= new CalculateTMAXFineLockWithDelay();
				CalculateTMAXFineLockWithDelay.stationMap = new HashMap<String,Station>();
				try {
					objFineLock.fineCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Fine Lock Execution With Delay");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with coarse lock on recordList as number of times as specified by 
	// iteration value, collects all data and finds min, max and average of collected data  	
	public static void coarseLockWithDelay(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXCoarseLockWithDelay objCoarseLock= new CalculateTMAXCoarseLockWithDelay();
				CalculateTMAXCoarseLockWithDelay.stationMap = new HashMap<String,Station>();
				try {
					objCoarseLock.coraseLockCalculation(choppedList);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("Coarse Lock Execution With Delay");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}
	
	// Function invokes execution of program with no sharing mechanism on recordList as number of times as specfied by 
	// iteration value, collects all data and finds min, max and average of collected data  		
	public static void noSharingLockWithDelay(List<List<String>> choppedList, Integer iteration){
		ArrayList<Long> runningTime = new ArrayList<Long>();
		for(int i=0;i<iteration;i++){
				Long start = System.currentTimeMillis();
				CalculateTMAXNoSharingWithDelay objNoSharing= new CalculateTMAXNoSharingWithDelay();
				try {
					objNoSharing.noSharingCalculation(choppedList);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Long end = System.currentTimeMillis();
				runningTime.add((end - start));
		}
		
		Collections.sort(runningTime);
		System.out.println("No Sharing Execution With Delay");
		System.out.println("Minimum Running Time: "+runningTime.get(0));
		System.out.println("Average Running Time: "+average(runningTime));
		System.out.println("Maximum Running Time: "+runningTime.get(runningTime.size()-1));
		System.out.println("----------------------------------------------------------------");
	}

	
	public static Double average(List<Long> element){
		Double a = 0.0;
		for(Long i : element){
			a+=i;
		}
		return a/element.size();
	}
}
