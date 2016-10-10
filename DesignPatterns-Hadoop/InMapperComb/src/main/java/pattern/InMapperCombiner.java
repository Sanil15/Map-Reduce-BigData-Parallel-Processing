package pattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.commons.Station;

import au.com.bytecode.opencsv.CSVParser;

// Program implements InMapperComb pattern, it creates a local aggregation of data in Mapper to significantly 
// reduce the work processing load on the reducer
public class InMapperCombiner {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length < 2){
			 System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
	         System.exit(2);
		}
		
		// Hadoop Job Configuration
		Job job = new Job(conf, "No Combiner Weather Data");
		job.setJarByClass(InMapperCombiner.class);
		job.setMapperClass(StationMapper.class);
		job.setReducerClass(TempratureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Station.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	// Class implements InMapperCombining design pattern and emits StationId as key
	// and Station object as value
	public static class StationMapper extends Mapper<Object, Text, Text, Station>{
		// word will store StationId i.e key output for Mapper
		private Text word = new Text();
		// Accumulating data structure for local aggregation i.e hashmap
		private HashMap<String,Station> records;
		
		// Initializing the map  
		public void setup(Context context){
			records = new HashMap<String,Station>();
		}
		
		// InMapperCombiner design pattern
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String stationDetail[] = new CSVParser().parseLine(value.toString());
			// Set StationId as key to be emitted 
			word.set(stationDetail[0]);
			
			// Only consider the records of type TMAX and TMIN, ignore others 
			if(stationDetail[2].equals("TMAX") || stationDetail[2].equals("TMIN")){
				
				// If the accumulating data structure does not contain an entry with its key as StationId of current record/line in 
				// consideration, make a new Station object and initialize the values of its attributes extracted from input line i.e stationDetail
				if(!records.containsKey(word.toString())){
					// Create a new Station Object
					Station stn = new Station(word, new DoubleWritable(0.0), new DoubleWritable(0.0), new IntWritable(0), new IntWritable(0));
					
					// If the type of input line record is TMAX, set the TMAX related attributes for station object 
					if(stationDetail[2].equals("TMAX")){
						stn.setTmaxTotal(new DoubleWritable(Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMAX(new IntWritable(1));
						records.put(word.toString(), stn);
					}
					// If the type of input line record is TMIN, set the TMIN related attributes for station object
					else if(stationDetail[2].equals("TMIN")){
						stn.setTminTotal(new DoubleWritable(Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMIN(new IntWritable(1));
						records.put(word.toString(), stn);
					}
				}
				
				// If the accumulating structure i.e map already contains entry for StationId, just update that entry with 
				// relevant values as input record type is i.e either "TMAX" or "TMIN"
				else{
					// Get the existing Station object by its key i.e StationId
					Station stn = records.get(word.toString());
					
					// If the type of input line record is TMAX, Update the TMAX related attributes for station object
					if(stationDetail[2].equals("TMAX")){
						stn.setTmaxTotal(new DoubleWritable(stn.getTmaxTotal().get() + Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMAX(new IntWritable(stn.getRecordCountTMAX().get()+1));
						records.put(word.toString(), stn);
					}
					// If the type of input line record is TMIN, Update the TMIN related attributes for station object
					else if(stationDetail[2].equals("TMIN")){
						stn.setTminTotal(new DoubleWritable(stn.getTminTotal().get() + Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMIN(new IntWritable(stn.getRecordCountTMIN().get() + 1));
						records.put(word.toString(), stn);
					}

				}
			}
			
		}
		
		// Method just emits all the HashMap entries as output of the map i.e
		// StationId as the key and Station object as the value
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Map.Entry<String,Station> obj:records.entrySet()){
				context.write(new Text(obj.getKey()), obj.getValue());
			}
		}
		
	}
	
	// Reducer takes Station Id as key and List of Station as input, iterates over each station to 
	// calculate tminAverage and tmaxAverage if the relevant records exist. 
	// It emits result as key that contains StationId, tminAverage and tmaxAverage if they exists, else 
	// tells No Records Found if either one of them doesn't
	public static class TempratureReducer extends Reducer<Text, Station, Text, NullWritable>{
		
		public void reduce(Text key, Iterable<Station> stationList, Context context) throws IOException, InterruptedException{
			
			// Accumulators for a StationId i.e key
			// maintains TMAX temperature total of all stations encountered for a key
			Double tmaxTotal = 0.0;
			// maintains TMIN temperature total of all station encountered for a key
			Double tminTotal = 0.0;
			// maintains total record count for type TMAX records encountered for a key
			Integer tmaxCount = 0;
			// maintains total record count for type TMIN records encountered for a key
			Integer tminCount = 0;
			
			// Iterate over the list of records to gather data
			// on accumulators
			for(Station temp: stationList){
				tmaxTotal += temp.getTmaxTotal().get();
				tminTotal += temp.getTminTotal().get();
				tminCount += temp.getRecordCountTMIN().get();
				tmaxCount += temp.getRecordCountTMAX().get();
			}
			
			// Initialization for tminAverage and tmaxAverage
			Double tmaxAverage = tmaxTotal/tmaxCount;
			Double tminAverage = tminTotal/tminCount;
			
			// It will be the output result
			Text result = new Text();
			
			// Calculate tmaxAverage if number of records of TMAX type are 
			// more than zero, otherwise set garbage value to tmaxAverage
			if(tmaxCount > 0)
				tmaxAverage = tmaxTotal/tmaxCount;
			else 
				tmaxAverage = -1.0;
			
			// Calculate tminAverage if number of records of TMIN type are 
			// more than zero, otherwise set garbage value to tminAverage
			if(tminCount > 0)
				tminAverage = tminTotal/tminCount;
			else
				tminAverage = -1.0;
						
			// if tmaxAverage and tminAverage have valid (if tminCount and tmaxCount exist with +ve value) 
			// values then set those value to result
			if(tmaxCount != 0 && tminCount != 0)
				result.set(key.toString()+", "+tminAverage+", "+tmaxAverage);
			
			// Otherwise if tminAverage is defined/valid but tmaxAverage is not valid(tmaxCount is zero), just 
			// set tminAverage and No TMAX records found to result 
			else if(tmaxCount == 0)
				result.set(key.toString()+", "+tminAverage+", "+"No TMAX Records Found");
			
			// Else Otherwise if tmaxAverage is defined/valid but tminAverage is not valid(tminCount is zero), just 
			// set tmaxAverage and No TMIN records found to result
			else if(tminCount == 0)
				result.set(key.toString()+", "+"No TMIN Records Found"+", "+tmaxAverage);
			// Emit the result and NULL	
			context.write(result, NullWritable.get());
		}
	}
}
