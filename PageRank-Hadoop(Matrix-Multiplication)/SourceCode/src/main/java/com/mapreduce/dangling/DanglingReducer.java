package com.mapreduce.dangling;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mapreduce.dangling.DanglingDriver.UpdateCounter;

// Dangling reducer does a summation of pair-wise dot product to update dangling counter
public class DanglingReducer extends Reducer<MatrixElement,DoubleWritable,Text,Text>{

	public void reduce(MatrixElement key, Iterable<DoubleWritable> values,Context ctx) throws IOException, InterruptedException {
	
		Integer counter = 0;
		Double danglingValue = 1.0d;
		for(DoubleWritable element: values){
			danglingValue = danglingValue * element.get();
			counter++;
		}
		
		// Counter check to calculate only dangling values * dangling page ranks
		// For dangling nodes only iterator will have two values
		if(counter == 2){
			long test = (long) (danglingValue * Math.pow(10, 16)); //convert to long
			ctx.getCounter(UpdateCounter.DANGLINGOFFSET).increment((test));
		}
	}
}
