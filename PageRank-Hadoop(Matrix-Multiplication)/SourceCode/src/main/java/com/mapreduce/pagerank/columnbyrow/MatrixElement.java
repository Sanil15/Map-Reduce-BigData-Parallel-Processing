package com.mapreduce.pagerank.columnbyrow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//Implements WritableComparable since it is emitted as key from RankMapper and AdjacencyMatrixMapper
public class MatrixElement implements WritableComparable {
	// Stores row or column id
	private LongWritable rowOrColumn;
	// Stores matrix id
	private Text matrix;
	
	public MatrixElement(){
		rowOrColumn = new LongWritable();
		matrix = new Text();
	}	
	
	public MatrixElement(LongWritable rowOrColumn, Text matrix) {
		super();
		this.rowOrColumn = rowOrColumn;
		this.matrix = matrix;
	}

	public LongWritable getRowOrColumn() {
		return rowOrColumn;
	}

	public void setRowOrColumn(LongWritable rowOrColumn) {
		this.rowOrColumn = rowOrColumn;
	}

	public Text getMatrix() {
		return matrix;
	}

	public void setMatrix(Text matrix) {
		this.matrix = matrix;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		rowOrColumn.readFields(in);
		matrix.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		rowOrColumn.write(out);
		matrix.write(out);

	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		MatrixElement other = (MatrixElement)o;
		Long col1 = this.getRowOrColumn().get();
		Long col2 = other.getRowOrColumn().get();
		
		if(col1.compareTo(col2) == 0){
			return other.getMatrix().toString().compareTo(this.getMatrix().toString());
		}
		return col1.compareTo(col2);
	}


	
}
