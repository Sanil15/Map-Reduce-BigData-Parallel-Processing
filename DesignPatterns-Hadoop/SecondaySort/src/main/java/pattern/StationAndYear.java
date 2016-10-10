package pattern;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
// Class serves as a Composite Key for the Mapper hence it 
// implements WritableComparable
public class StationAndYear implements WritableComparable {
	// Unique StationId for the input
	private Text stationId;
	// Year of the file which data belongs to
	private IntWritable year;
	
	public StationAndYear(){
		this.stationId = new Text();
		this.year = new IntWritable();
	}
	
	public StationAndYear(Text stationId, IntWritable year) {
		super();
		this.stationId = stationId;
		this.year = year;
	}
	public Text getStationId() {
		return stationId;
	}
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}
	public IntWritable getYear() {
		return year;
	}
	public void setYear(IntWritable year) {
		this.year = year;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		stationId.readFields(in);
		year.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		stationId.write(out);
		year.write(out);
	}
	
	
	@Override
	public int hashCode(){
		return stationId.hashCode()+year.get();
	}

	// Sorts in increasing order of stationId
	// if stationId is equal then sort in increasing order of year next
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		  StationAndYear o1 = this;
	      StationAndYear o2 = (StationAndYear) o;
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
