package com.mapreduce.cleansing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** CleansingReducer segregates the data into 75% TRAIN and 25% TEST data based on random sampling   
 * */

public class CleansingReducer extends Reducer<Text, Text, Text, NullWritable> {

	// Configure Multiple Outputs
	private MultipleOutputs mos;

	public void setup(Context context) {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Random r = new Random();
		for (Text value : values) {

			// segregate records into TRAIN and TEST
			if (r.nextDouble() <= 0.75) {
				mos.write("TRAIN", new Text(value.toString()),
						NullWritable.get());
			} else
				mos.write("TEST", new Text(value.toString()),
						NullWritable.get());
		}

	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
