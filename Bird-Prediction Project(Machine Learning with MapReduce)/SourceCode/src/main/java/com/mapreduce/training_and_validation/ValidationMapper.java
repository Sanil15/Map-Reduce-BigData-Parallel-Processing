package com.mapreduce.training_and_validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;

/** Validation of TEST DATA (sampled by cleaning job) is a MAP-ONLY JOB using HORIZONTAL STRIPES APPROACH. 
 *  
 *  ValidationMapper 
 *   - loads K models into memory, through DISTRIBUTED CACHE
 *   - Each model in the ensemble will calculate its own prediction value for test record, final value is just average of aggregated value over all the models
 *   - Used LIBLINEAR for prediction
 *     
 *  */

public class ValidationMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	int numOfFeatures;
	private List<Model>models = new ArrayList<>();
	
	public void setup(Context ctx) throws IOException{
		
		// load k models into memory
		
		try{
			String cachePath = ctx.getConfiguration().get("models");
			for(int j = 0;j < 10; j++){
				String readPath  = cachePath+"/part-r-0000"+j;
				
				Path pt=new Path(readPath);
				FileSystem fs = FileSystem.get(URI.create(cachePath),ctx.getConfiguration());
				FileStatus[] status = fs.listStatus(pt);
				for (int i=0;i<status.length;i++){
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					models.add(Model.load(br));

				}
			}

		} catch(Exception ex) {
			System.err.println("Exception in mapper setup:" + ex.getMessage());
		}

	}
	
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String data = value.toString();
		numOfFeatures = data.split(",").length - 1;
		Feature[] instance = prepareFeatureNodes(data);
		double predictions = 0.0;
		
		// calculate prediction of record from each of the K Models, and accumulate the scores
		for(Model m : models){
			
			double prediction = Linear.predict(m, instance);
			
			predictions+=prediction;
		}
		
		// compute average value of the accumulated prediction
		Double avgPred = predictions/10;
		
		context.write(new Text(data.split(",")[0].split(":")[1]), new DoubleWritable(avgPred));
		
	}

	private Feature[] prepareFeatureNodes(String data) {

		String[] tokens = data.split(",");
		Feature[] instance = new Feature[numOfFeatures];
		for (int i = 1; i < tokens.length; i++) {
			
			String[] values =  tokens[i].split(":");
			instance[i - 1]  = new FeatureNode(Integer.parseInt(values[0]),
					Double.parseDouble(values[1]));
		}
		return instance;
				
	}
}