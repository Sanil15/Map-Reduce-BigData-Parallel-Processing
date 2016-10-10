package com.mapreduce.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Class accumulates Weather data as a Record
// It implements Writable since it s used as value emitted out of Mapper
public class Record implements Writable{

	// Unique Station Id for the record 
	private Text stationId;
	//  Value for the record 
	private DoubleWritable val;
	// Type of value i.e it will be either TMAX or TMIN
	// we ignore all other types
	private Text type;
	// Count of records for which object is holding data 
	private IntWritable count;
	
	public Record(){
		this.stationId = new Text();
		this.val = new DoubleWritable();
		this.type = new Text();
		this.count = new IntWritable();
	}

	
	public Record(Text stationId, DoubleWritable val, Text type, IntWritable count) {
		this.stationId = stationId;
		this.val = val;
		this.type = type;
		this.count = count;
	}

	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}

	public DoubleWritable getVal() {
		return val;
	}

	public void setVal(DoubleWritable val) {
		this.val = val;
	}

	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}
	
	

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stationId.readFields(in);
		val.readFields(in);
		type.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		stationId.write(out);
		val.write(out);
		type.write(out);
		count.write(out);
	}
	
	@Override
	public int hashCode(){
		return stationId.hashCode();
	}

	@Override
	public String toString() {
		return "Record [stationId=" + stationId + ", val=" + val + ", type=" + type + ", count=" + count + "]";
	}

	
}

