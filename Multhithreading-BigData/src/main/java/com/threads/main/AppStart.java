package com.threads.main;

import java.net.URISyntaxException;
import java.util.List;
import com.threads.commons.ReadUtils;

public class AppStart {
	public static final int ITERATION = 10;
	
	public static void main(String args[]) throws InterruptedException, URISyntaxException{
		List<String> recordList = ReadUtils.loader(args[0]);
		List<List<String>> choppedList = ReadUtils.chopLists(recordList, 4);
		
        // Sequential Calculation 
		RunFactory.sequential(recordList, ITERATION);
		
		// No Lock Calculation 
		RunFactory.noLock(choppedList,ITERATION);
		
		// Fine Lock Calculation
		RunFactory.fineLock(choppedList, ITERATION);
		
        // Coarse Lock Calculation
		RunFactory.coarseLock(choppedList, ITERATION);
		
		// NO Sharing Calculation
		RunFactory.noSharingLock(choppedList, ITERATION);
		
		// Sequential Calculation With Delay
		RunFactory.sequentialWithDelay(recordList, ITERATION);
				
		// No Lock Calculation With Delay
		RunFactory.noLockWithDelay(choppedList,ITERATION);
				
		// Fine Lock Calculation With Delay
		RunFactory.fineLockWithDelay(choppedList, ITERATION);
				
		// Coarse Lock Calculation With Delay
		RunFactory.coarseLockWithDelay(choppedList, ITERATION);
				
		// NO Sharing Calculation With Delay
		RunFactory.noSharingLockWithDelay(choppedList, ITERATION);
	}
}
