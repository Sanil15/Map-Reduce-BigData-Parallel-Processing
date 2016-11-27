package com.mapreduce.pagerank.columnbyrow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mapreduce.dangling.DanglingDriver.UpdateCounter;

// Reducer receives M[k,i] and R[k,0] for different i's. It emits all the product 
// M[k,i] * R[k,0] with a key(i) because this is the contribution for the cell [i,0]
// Since R is |V| X 1 matrix there should be only element from matrix "R" ~  with any number of elements 
// from matrix "M" for a single row(pageID) which is the key 
public class PageRankReducer extends Reducer<MatrixElement,MatrixTuple,Text,Text>{

	// Sort Comparator ensures that all the MatrixTuple that have matrixId "R" end up first in the reduce call as compared to ones
	// with matrixID as "M". If first element is not of id "R" that suggests that key is a dead node. That is it has no page rank
	// value in vector R but has its name in Matrix M i.e it only has outlinks and no inlinks so we reproduce its page rank value.
	// Otherwise every element from M is multiplied with every element of R.
	public void reduce(MatrixElement key, Iterable<MatrixTuple> values,Context ctx) throws IOException, InterruptedException {

		// Get the counter values
		Long pageCount = ctx.getConfiguration().getLong("pageNameCount", 0L);
		Long danglingLong = ctx.getConfiguration().getLong("danglingOffset", 0L);

		Double rValue = null;
		Integer count = 0;

		for(MatrixTuple tuple: values){
			if(count == 0){
				// Found a Dead Node i.e a node with outlinks but no inlinks. Such nodes exist in M matrix but not
				// in the R vector
				if(key.getMatrix().toString().equals("M")){
					
					// Calculate rank value for dead node, note here Σ p(m)/c(m) = 0
					Double danglingPR = (double)danglingLong / Math.pow(10, 16);
					Double pageRankValue = ((PageRankDriver.DAMPINGFACTOR / pageCount) + ((1-PageRankDriver.DAMPINGFACTOR)*(danglingPR)));
					rValue = pageRankValue;

					// Account for the Dead Node in the intermediate file 
					ctx.write(new Text(Long.toString(key.getRowOrColumn().get())), new Text(Double.toString(0.0d)));

					MatrixTuple mTuple = tuple;
					Double dotProduct = mTuple.getValue().get() * rValue;

					// Emit for the M[k,i] * R[k,0] value with i as intermediate key
					ctx.write(new Text(Long.toString(mTuple.getRowOrColumnId().get())), new Text(Double.toString(dotProduct)));
					
				}
				// Not a dead node so just fetch the page rank value;
				else{
					MatrixTuple rTuple = tuple;
					rValue = rTuple.getValue().get();
				}
				count++;
			}
			// Just continue calculating  M[k,i] * R[k,0] for all possible i values as received
			else{
				MatrixTuple mTuple = tuple;
				Double dotProduct = mTuple.getValue().get() * rValue;
				
				// Emit for the M[k,i] * R[k,0] value with i as intermediate key
				ctx.write(new Text(Long.toString(mTuple.getRowOrColumnId().get())), new Text(Double.toString(dotProduct)));
			}
		}
	}

}
