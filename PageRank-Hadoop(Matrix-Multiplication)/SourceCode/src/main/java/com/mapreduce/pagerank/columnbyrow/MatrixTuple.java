package com.mapreduce.pagerank.columnbyrow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// Implements Writable since it is emitted as value from RankMapper and AdjacencyMatrixMapper
public class MatrixTuple implements Writable {

	// Stores row or column id
	private LongWritable rowOrColumnId;
	// Stores value
	private DoubleWritable value;
	
	public MatrixTuple(){
		rowOrColumnId = new LongWritable();
		value = new DoubleWritable();
	}
		
	public LongWritable getRowOrColumnId() {
		return rowOrColumnId;
	}
	public void setRowOrColumnId(LongWritable rowOrColumnId) {
		this.rowOrColumnId = rowOrColumnId;
	}
	public DoubleWritable getValue() {
		return value;
	}
	public void setValue(DoubleWritable value) {
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		rowOrColumnId.readFields(in);
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
	
		rowOrColumnId.write(out);
		value.write(out);
	}
	
}
