package pattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.mapreduce.commons.Station;

import au.com.bytecode.opencsv.CSVParser;

// Program implements Secondary sort design pattern for Weather Data Analysis  
// It does InMapperCombining to improve efficiency and uses composite key from Mapper 
// i.e StationAndYear, it also uses a Key Comparator and a Group comparator 
// It sorts the StationIds in a way such that aggregation of complete data in reducer becomes
// easy without any use of addition accumulating data structures in reducer
public class SecondarySortStation {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();

		String otherArgs[] = new GenericOptionsParser(conf,args).getRemainingArgs();

		if(otherArgs.length < 2){
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}

		// Hadoop job configuration
		Job job = new Job(conf, "Seconday Sort Weather Data");
		job.setJarByClass(SecondarySortStation.class);
		job.setMapperClass(StationMapper.class);
		// Setting the KeyComparator
		job.setSortComparatorClass(KeyComparator.class);
		// Setting the GroupingComparator
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(TempratureReducer.class);
		// Composite Key Output from Mapper
		job.setMapOutputKeyClass(StationAndYear.class);
		job.setMapOutputValueClass(Station.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	// Class implements InMapperCombining design pattern
	public static class StationMapper extends Mapper<Object, Text, StationAndYear, Station>{
		// word will store StationId i.e a part of Composite Key emitted by Mapper
		private Text word = new Text();
		// year will store Year of file/record under process, also a part of Composite Key emitted by Mapper 
		private IntWritable year = new IntWritable(); 
		// Accumulating data structure for local aggregation, map
		private HashMap<String,Station> records;

		// Initializing the map  
		public void setup(Context context){
			records = new HashMap<String,Station>();
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			fileName = fileName.replace(".csv", "");
			// Set Year i.e a part of composite key to be emitted
			year = new IntWritable(Integer.parseInt(fileName));
			String stationDetail[] = new CSVParser().parseLine(value.toString());
			// Set StationId i.e a part of composite key to be emitted 
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
					}
					// If the type of input line record is TMIN, set the TMIN related attributes for station object
					else if(stationDetail[2].equals("TMIN")){
						stn.setTminTotal(new DoubleWritable(Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMIN(new IntWritable(1));
					}
					records.put(word.toString(), stn);
				}
				
				// If the accumulating structure i.e map already contains entry for StationId, just update that entry with 
				// relevant values as input record type is
				else{
					// Get the existing Station object by its key StationId
					Station stn = records.get(word.toString());
					
					// If the type of input line record is TMAX, Update the TMAX related attributes for station object
					if(stationDetail[2].equals("TMAX")){
						stn.setTmaxTotal(new DoubleWritable(stn.getTmaxTotal().get() + Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMAX(new IntWritable(stn.getRecordCountTMAX().get()+1));
					}
					// If the type of input line record is TMIN, Update the TMIN related attributes for station object
					else if(stationDetail[2].equals("TMIN")){
						stn.setTminTotal(new DoubleWritable(stn.getTminTotal().get() + Double.parseDouble(stationDetail[3])));
						stn.setRecordCountTMIN(new IntWritable(stn.getRecordCountTMIN().get() + 1));
					}
					records.put(word.toString(), stn);
				}
			}

		}

		// Method just emits all the HashMap entries as output of the map with a change i.e
		// (StationId,Year) as the composite key of the type StationAndYear and Station object as the value
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Map.Entry<String,Station> obj:records.entrySet()){
				context.write(new StationAndYear(new Text(obj.getKey()),year), obj.getValue());

			}
		}

	}
	
	// Key Comparator sorts in increasing order of stationId
	// if stationId is equal then sort in increasing order of year next
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(StationAndYear.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			StationAndYear o1 = (StationAndYear) w1;
			StationAndYear o2 = (StationAndYear) w2;
			// Get the difference of stationId
			int diffStationId = o1.getStationId().toString().compareTo(o2.getStationId().toString()) ;
			// Get the difference of Year
			int diffYear = o1.getYear().get() - o2.getYear().get();

			// If difference of stationId is equal, then return difference of year
			// Otherwise return difference of stationId
			if(diffStationId == 0)
				return diffYear;
			else
				return diffStationId;
		}

	}

	// Grouping Comparator Sorts in the increasing order of StationId
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(StationAndYear.class,true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			StationAndYear st1 = (StationAndYear) w1;
			StationAndYear st2 = (StationAndYear) w2;
			// compare on the basis of StationId
			return st1.getStationId().toString().compareTo(st2.getStationId().toString());
		}

	}


	// Since we are using Secondary Sort design pattern, all the inputs to the one reduce call will have the same
	// StationId in the composite key and the order of input will be in the increasing order of Year in the composite key
	// We take advantage of this ordering to just accumulate inputs with same Year for same StationId without having any 
	// additional data structure or lookup.
	public static class TempratureReducer extends Reducer<StationAndYear, Station, Text, Text>{
		public void reduce(StationAndYear key, Iterable<Station> stationList, Context context) throws IOException, InterruptedException{
			// It will maintain the Year in the composite key of last input to reducer 
			int previousYear = 0;
			// It will maintain the Year in the composite key of current input to reducer
			int currentYear = key.getYear().get();

			// Accumulators for a StationId input with same Year
			// maintains TMAX total of all stations with encountered for a StationId with same Year
			Double tmaxTotal = 0.0;
			// maintains TMIN total of all station encountered for a StationId with same Year
			Double tminTotal = 0.0;
			// maintains total record count for type TMAX all station encountered for a StationId with same Year
			Integer tmaxCount = 0;
			// maintains total record count for type TMIN all station encountered for a StationId with same Year
			Integer tminCount = 0;

			// Initialize StringBuilder 
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			
			// Iterates over the list of Station and accumulates data for Same StationId with Same Year
			// Maintains two variables currentYear and previousYear, it adds to accumulator if both are same
			// otherwise appends value to output by calculating tminAverage and tmaxAverage for previousYear records,
			// since records are also in sorted order of year and reset all the accumulators
			for(Station temp: stationList){

				previousYear = currentYear;
				currentYear = key.getYear().get();

				// If the current input record has a changed year, calculate tminAverage and
				// tmaxAverage from accumulators and append to output 
				if(currentYear != previousYear){
					Double tmaxAverage = 0.0;
					Double tminAverage = 0.0;

					// Calculate tmaxAverage if number of records of TMAX type are 
					// more than zero, otherwise set garbage value to tmaxCount
					if(tmaxCount > 0)
						tmaxAverage = tmaxTotal/tmaxCount;
					else
						tmaxCount = -1;

					// Calculate tminAverage if number of records of TMIN type are 
					// more than zero, otherwise set garbage value to tminCount
					if(tminCount > 0)
						tminAverage = tminTotal/tminCount;
					else
						tminCount = -1;

					// if tmaxAverage and tminAverage have valid (if tminCount and tmaxCount exist with +ve value) 
					// values then set those value to result
					if(tmaxCount!=-1 && tminCount!=-1)
						sb.append("("+previousYear+", "+tminAverage+", "+tmaxAverage+"),");


					// Otherwise if tminAverage is defined/valid but tmaxAverage is not valid(tmaxCount is garbage i.e -1), just 
					// set tminAverage and No TMAX records found to result 
					else if(tmaxCount == -1)
						sb.append("("+previousYear+", "+tminAverage+", "+" No TMAX Record Found"+"),");

					// Else Otherwise if tmaxAverage is defined/valid but tminAverage is not valid(tminCount is garbage i.e -1), just 
					// set tmaxAverage and No TMIN records found to result
					else if(tminCount == -1)
						sb.append("("+previousYear+", "+"No TMIN Record Found"+", "+tmaxAverage+"),");

					// Reset all accumulators
					tmaxTotal = 0.0;
					tminTotal = 0.0;
					tmaxCount = 0;
					tminCount = 0;


				}

				// keep on accumulating relevant values
				tmaxTotal += temp.getTmaxTotal().get();
				tminTotal += temp.getTminTotal().get();
				tminCount += temp.getRecordCountTMIN().get();
				tmaxCount += temp.getRecordCountTMAX().get();


			}

			// Append the output for data for last Year for the StationId encountered
			if(currentYear == previousYear){
				Double tmaxAverage = 0.0;
				Double tminAverage = 0.0;

				// Calculate tmaxAverage if number of records of TMAX type are 
				// more than zero, otherwise set garbage value to tmaxCount
				if(tmaxCount > 0)
					tmaxAverage = tmaxTotal/tmaxCount;
				else
					tmaxCount = -1;

				// Calculate tminAverage if number of records of TMIN type are 
				// more than zero, otherwise set garbage value to tminCount
				if(tminCount > 0)
					tminAverage = tminTotal/tminCount;
				else
					tminCount = -1;

				// if tmaxAverage and tminAverage have valid (if tminCount and tmaxCount exist with +ve value) 
				// values then set those value to result
				if(tmaxCount!=-1 && tminCount!=-1)
					sb.append("("+currentYear+", "+tminAverage+", "+tmaxAverage+")");

				// Otherwise if tminAverage is defined/valid but tmaxAverage is not valid(tmaxCount is garbage i.e -1), just 
				// set tminAverage and No TMAX records found to result 
				else if(tmaxCount == -1)
					sb.append("("+currentYear+", "+tminAverage+", "+" No TMAX Record Found"+")");

				// Else Otherwise if tmaxAverage is defined/valid but tminAverage is not valid(tminCount is garbage i.e -1), just 
				// set tmaxAverage and No TMIN records found to result
				else if(tminCount == -1)
					sb.append("("+currentYear+", "+"No TMIN Record Found"+", "+tmaxAverage+")");

			}

			sb.append("]");
			Text result = new Text();
			result.set(sb.toString());
			// Emit the StationId as key and the result of StringBuilder sb as value;
			context.write(key.getStationId(), result);


		}
	}
}
