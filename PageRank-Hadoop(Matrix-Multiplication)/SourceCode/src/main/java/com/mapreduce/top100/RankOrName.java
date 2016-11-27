package com.mapreduce.top100;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// RankOrName implements Writable since it is emitted as a value from Top100Mapper and NameMapper
public class RankOrName implements Writable {

	// Will represent either "Name" or "Rank"
	private Text type;
	// Will represent value;
	private Text value;
	
	public RankOrName(){
		type = new Text();
		value = new Text();
	}
	
	public RankOrName(Text type, Text value) {
		super();
		this.type = type;
		this.value = value;
	}


	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		type.readFields(in);
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	
		type.write(out);
		value.write(out);
	}
}
