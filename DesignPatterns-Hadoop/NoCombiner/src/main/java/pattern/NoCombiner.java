package pattern;

import java.io.IOException;
import com.mapreduce.commons.Record;

import au.com.bytecode.opencsv.CSVParser;

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

// Program implements NoCombiner pattern that is just a simple Mapper and a Reducer for 
// Weather Data Analysis 
public class NoCombiner {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if(otherArgs.length < 2){
			 System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
	         System.exit(2);
		}
		// Job Configuration for Hadoop
		Job job = new Job(conf, "No Combiner Weather Data");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(StationMapper.class);
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
	
	// Mapper emits StationId as key which is StationId and Record object as value containing station details
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
				Record obj = new Record(new Text(stationDetail[0]), new DoubleWritable(Double.parseDouble(stationDetail[3])), new Text("TMIN"), new IntWritable(1));
				context.write(word, obj);
			}
		}
		
	}

	// Reducer takes Station Id as key and List of records as input, iterates over record to 
	// calculate tminAverage and tmaxAverage if the relevant records exist, it emits result as key that 
	// contains StationId, tminAverage and tmaxAverage if they exists, else 
	// tells No Records Found if either one of them doesn't for a Station
	public static class TempratureReducer extends Reducer<Text, Record, Text, NullWritable>{
		public void reduce(Text key, Iterable<Record> records, Context context) throws IOException, InterruptedException{
			
			// Accumulators for a StationId i.e key
			// maintains record count for type TMIN
			Integer tminCount = 0;
			// maintains TMIN temperature value total of all records encountered
			Double tminTotal = 0.0;
			// maintains record count for type TMAX
			Integer tmaxCount = 0;
			// maintains TMAX temperature value total of all records encountered
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
			
			// Otherwise if tminAverage is defined/valid but tmaxAverage is not valid, just 
			// set tminAverage and No TMAX records found to result
			else if(tmaxAverage == -1.0)
				result.set(key.toString()+", "+tminAverage+", "+"No TMAX Records Found");
			
			// Else Otherwise if tmaxAverage is defined/valid but tminAverage is not valid, just 
			// set tmaxAverage and No TMIN records found to result
			else if(tminAverage == -1.0)
				result.set(key.toString()+", "+"No TMIN Records Found"+", "+tmaxAverage);
			
			// Emit the result and NULL
			context.write(result, NullWritable.get());

		}
	}
}
