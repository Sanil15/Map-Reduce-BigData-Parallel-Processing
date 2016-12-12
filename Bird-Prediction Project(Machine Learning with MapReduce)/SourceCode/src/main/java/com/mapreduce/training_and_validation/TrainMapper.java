package com.mapreduce.training_and_validation;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/** TrainMapper: 
 *  From our training data set D, we created bagged ensemble that is trained as follows: 
	- Created k = 10 independent bootstrap samples D1, D2,..., Dk of D.
	- Each training record has a probability of 1-(1-1/n)n selection, for large n (here 1701975 ~ 1.7 million) this converges to 0.63 

 * */

public class TrainMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	static final Integer TOTAL_BAGS = 10;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Random r = new Random();
		for (int i = 0; i < TOTAL_BAGS; i++) {
			if (r.nextDouble() <= 0.63) {
				context.write(new IntWritable(i), value);
			}
		}
	}
}