package com.mapreduce.dangling;

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


public class MatrixElement implements WritableComparable {

	private LongWritable i = new LongWritable();
	private LongWritable j = new LongWritable();
	private LongWritable k = new LongWritable();

	public MatrixElement(){
	}


	public MatrixElement(LongWritable i, LongWritable j, LongWritable k) {
		this.i = i;
		this.j = j;
		this.k = k;
	}

	public LongWritable getI() {
		return i;
	}

	public void setI(LongWritable i) {
		this.i = i;
	}


	public LongWritable getJ() {
		return j;
	}

	public void setJ(LongWritable j) {
		this.j = j;
	}

	public LongWritable getK() {
		return k;
	}

	public void setK(LongWritable k) {
		this.k = k;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		i.readFields(in);
		j.readFields(in);
		k.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		i.write(out);
		j.write(out);
		k.write(out);
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((i == null) ? 0 : i.hashCode());
		result = prime * result + ((j == null) ? 0 : j.hashCode());
		result = prime * result + ((k == null) ? 0 : k.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MatrixElement other = (MatrixElement) obj;
		if (i == null) {
			if (other.i != null)
				return false;
		} else if (!i.equals(other.i))
			return false;
		if (j == null) {
			if (other.j != null)
				return false;
		} else if (!j.equals(other.j))
			return false;
		if (k == null) {
			if (other.k != null)
				return false;
		} else if (!k.equals(other.k))
			return false;
		return true;
	}


	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		MatrixElement other = (MatrixElement)o;
		Long iDiff = this.getI().get() - other.getI().get();
		Long jDiff = this.getJ().get() - other.getJ().get();
		Long kDiff = this.getK().get() - other.getK().get();

		if(iDiff > 0)
			return iDiff.intValue();

		else if(jDiff > 0)
			return jDiff.intValue();

		else
			return kDiff.intValue();
	}


	@Override
	public String toString() {
		return "MatrixElement [i=" + i + ", j=" + j + ", k=" + k + "]";
	}




}
