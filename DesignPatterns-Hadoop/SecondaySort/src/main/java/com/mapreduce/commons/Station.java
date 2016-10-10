package com.mapreduce.commons;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
//Class accumulates weather data records as Station for single StationId
// It implements Writable since it is emitted as value from Map
public class Station implements Writable{
	// Unique StationId of a Station
	private Text stationId;
	// Maintains the total value of TMIN temperatures
	private DoubleWritable tminTotal;
	// Maintains the total value of TMAX temperatures 
	private DoubleWritable tmaxTotal;
	// Maintains the record count of type TMIN
	private IntWritable recordCountTMIN;
	// Maintains the record count of type TMAX
	private IntWritable recordCountTMAX;
	
	public Station(){
		stationId = new Text();
		tminTotal = new DoubleWritable();
		tmaxTotal = new DoubleWritable();
		recordCountTMAX = new IntWritable();
		recordCountTMIN = new IntWritable();
	}
	
	public Station(Text stationId, DoubleWritable tminTotal, DoubleWritable tmaxTotal, IntWritable recordCountTMIN,
			IntWritable recordCountTMAX) {
		this.stationId = stationId;
		this.tminTotal = tminTotal;
		this.tmaxTotal = tmaxTotal;
		this.recordCountTMIN = recordCountTMIN;
		this.recordCountTMAX = recordCountTMAX;
	}
	
	public Text getStationId() {
		return stationId;
	}
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}
	public DoubleWritable getTminTotal() {
		return tminTotal;
	}
	public void setTminTotal(DoubleWritable tminTotal) {
		this.tminTotal = tminTotal;
	}
	public DoubleWritable getTmaxTotal() {
		return tmaxTotal;
	}
	public void setTmaxTotal(DoubleWritable tmaxTotal) {
		this.tmaxTotal = tmaxTotal;
	}
	
	public IntWritable getRecordCountTMIN() {
		return recordCountTMIN;
	}
	public void setRecordCountTMIN(IntWritable recordCountTMIN) {
		this.recordCountTMIN = recordCountTMIN;
	}
	public IntWritable getRecordCountTMAX() {
		return recordCountTMAX;
	}
	public void setRecordCountTMAX(IntWritable recordCountTMAX) {
		this.recordCountTMAX = recordCountTMAX;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		stationId.readFields(in);
		tminTotal.readFields(in);
		tmaxTotal.readFields(in);
		recordCountTMIN.readFields(in);
		recordCountTMAX.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		stationId.write(out);
		tminTotal.write(out);
		tmaxTotal.write(out);
		recordCountTMIN.write(out);
		recordCountTMAX.write(out);
	}
	
	@Override
	public int hashCode(){
		return stationId.hashCode();
	}

	@Override
	public String toString() {
		return "Station [stationId=" + stationId + ", tminTotal=" + tminTotal + ", tmaxTotal=" + tmaxTotal
				+ ", recordCountTMIN=" + recordCountTMIN + ", recordCountTMAX=" + recordCountTMAX + "]";
	}

	
}
