package com.mapreduce.top100;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// NameMapper just emits (pageId,pageName)
public class NameMapper extends Mapper<Object, Text, LongWritable, RankOrName>{
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		String []pageDetails = value.toString().split("\t");
		RankOrName rankOrName = new RankOrName(new Text("Name"), new Text(pageDetails[0]));
		ctx.write(new LongWritable(Long.parseLong(pageDetails[1])), rankOrName);
	}
}
