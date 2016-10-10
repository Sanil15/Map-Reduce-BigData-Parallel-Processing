package pattern;

import java.io.IOException;

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
import com.mapreduce.commons.Record;
import au.com.bytecode.opencsv.CSVParser;

// Program implements a combiner pattern i.e in addition to a Mapper and a Reducer 
// It also implements a combiner to reduce the work load of reducer
public class Combiner {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length < 2){
			 System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
	         System.exit(2);
		}

		// Job Configuration for Hadoop
		Job job = new Job(conf, "Combiner Weather Data");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(StationMapper.class);
		job.setCombinerClass(StationCombiner.class);
		job.setReducerClass(TempratureReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	

	// Mapper emits StationId as key and Record object as value containing station details
	public static class StationMapper extends Mapper<Object, Text, Text, Record>{
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			// Parse the station details from the input line
			String stationDetail[] = new CSVParser().parseLine(value.toString());
			
			// If input line type is of TMAX feed the station details as a Record object with TMAX type and emit it 
			if(stationDetail[2].equals("TMAX")){
				word.set(stationDetail[0]);
				Record obj = new Record(new Text(stationDetail[0]), new DoubleWritable(Double.parseDouble(stationDetail[3])), new Text("TMAX"),new IntWritable(1));
				context.write(word, obj);
			}
			// If input line type is of TMIN feed the station details as a Record object with TMIN type and emit it 
			else if(stationDetail[2].equals("TMIN")){
				word.set(stationDetail[0]);
				Record obj = new Record(new Text(stationDetail[0]), new DoubleWritable(Double.parseDouble(stationDetail[3])), new Text("TMIN"),new IntWritable(1));
				context.write(word, obj);
			}
		}
		
	}
	
	// Intermediate Combiner that just accumulates similar records i.e of same type(either "TMAX" or "TMIN") into
	// respective objects and emits those objects hence decreases the work of reducer
	public static class StationCombiner extends Reducer<Text, Record, Text, Record>{
		public void reduce(Text key, Iterable<Record> records, Context context) throws IOException, InterruptedException{
		
			// Accumulators for Combiner 
			// maintains count of record type TMIN
			Integer tminCount = 0;
			// maintains total temperature value for record type TMIN 
			Double tminVal = 0.0;
			// maintains count of record type TMAX
			Integer tmaxCount = 0;
			// maintains total temperature value for record type TMAX 
			Double tmaxVal = 0.0;
			
			// Iterate over the list of records to gather data
			// on accumulators
			for(Record temp: records){
				if(temp.getType().toString().equals("TMAX")){
					tmaxVal += temp.getVal().get();
					tmaxCount += 1;
				}				
				else if(temp.getType().toString().equals("TMIN")){
					tminVal += temp.getVal().get();
					tminCount += 1;
				}
				
			}
			
			// Feed the accumulated values for TMAX type and TMIN type on two objects namely max and min of
			// the type Record 
			Record max = new Record(key,new DoubleWritable(tmaxVal),new Text("TMAX"),new IntWritable(tmaxCount));
			Record min = new Record(key,new DoubleWritable(tminVal),new Text("TMIN"),new IntWritable(tminCount));
	
			// Emit both kinds of object i.e 
			// Emit Accumulated values in record of type TMAX
			context.write(key, max);
			// Emit Accumulated values in record of type TMIN
			context.write(key, min);
		}
	}
	
	// Reducer takes Station Id as key and List of records as input, iterates over record to 
	// determine tminAverage and tmaxAverage if the records exist.  
	// It emits result as key that contains StationId, tminAverage and tmaxAverage 
	// if they exists, else tells No Records Found if either one of them doesn't
	public static class TempratureReducer extends Reducer<Text, Record, Text, NullWritable>{
		public void reduce(Text key, Iterable<Record> records, Context context) throws IOException, InterruptedException{
			
			// Accumulators for a StationId i.e key
			// maintains record count for type TMIN
			Integer tminCount = 0;
			// maintains TMIN temperature total of all records encountered
			Double tminTotal = 0.0;
			// maintains record count for type TMAX
			Integer tmaxCount = 0;
			// maintains TMAX temperature total of all records encountered
			Double tmaxTotal = 0.0;
			// Initialization for tminAverage and tmaxAverage
			Double tmaxAverage = 0.0;
			Double tminAverage = 0.0;
			
			// Iterate over the list of records to gather data
			// on accumulators
			for(Record temp: records){		
				if(temp.getType().toString().equals("TMAX")){
					tmaxTotal += temp.getVal().get();
					tmaxCount += temp.getCount().get();
				}
				else if(temp.getType().toString().equals("TMIN")){
					tminTotal +=  temp.getVal().get();
					tminCount +=  temp.getCount().get();
				}
				
			}
			
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
			
			
			// It will be the output result
			Text result = new Text();
			
			// if tmaxAverage and tminAverage have valid values then set those value to result
			if(tmaxAverage != -1.0 && tminAverage != -1.0)
				result.set(key.toString()+", "+tminAverage+", "+tmaxAverage);
			
			// Otherwise if tminAverage is defined/valid but tmaxAverage is not valid(garbage value), just 
			// set tminAverage and No TMAX records found to result 
			else if(tmaxAverage == -1.0)
				result.set(key.toString()+", "+tminAverage+", "+"No TMAX Records Found");
			
			// Else Otherwise if tmaxAverage is defined/valid but tminAverage is not valid(garbage value), just 
			// set tmaxAverage and No TMIN records found to result
			else if(tminAverage == -1.0)
				result.set(key.toString()+", "+"No TMIN Records Found"+", "+tmaxAverage);
			
			// Emit the result and NULL
			context.write(result, NullWritable.get());
		}
	}
}
