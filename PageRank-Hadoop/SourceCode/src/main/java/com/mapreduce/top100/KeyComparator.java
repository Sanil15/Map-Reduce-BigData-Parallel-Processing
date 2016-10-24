package com.mapreduce.top100;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
// Key comparator just sorts the key in decreasing order (highest first)
public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
		super(DoubleWritable.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable o1 = (DoubleWritable) w1;
			DoubleWritable o2 = (DoubleWritable) w2;
			
			if(o2.get() - o1.get() > 0)
				return 1;
			
			else if(o2.get() - o1.get() < 0)
				return -1;
			
			else
				return 0;
		}

	}
  
