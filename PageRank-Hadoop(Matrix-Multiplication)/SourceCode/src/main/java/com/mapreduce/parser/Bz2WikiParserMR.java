package com.mapreduce.parser;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Bz2WikiParser is configured as a MapReduce Job, it reads the file input of bz2 compressed type
// checks for inconsistencies, removes them. It is a job that uses a single reducer as it provides all the 
// nodes with a unique id. It is configured to produce four files
// OutputM-r-0000 : Sparse Matrix Representation for graph, each line represents: PageId (OutlinkId1,1/adjacencyList.length)~(OutlinkId2,1/adjacencyList.length)~(OutlinkId3,1/adjacencyList.length)... 
// OutputD-r-0000 : Represents compact list of pageId which are dangling nodes and value(1/|V|) where |V| is total nodes in the graph, each line is in the form: PageId 1/|V|
// OutputR-r-0000 : Represents vector 𝐑(0) having all nodes in graph with the initial PageRank values 1/|V|  where |V| is total nodes in the graph, each line in the form: PageId 1/|V|
// OutputN-r-0000 : Represents Name to pageId mapping to read in the Top100 job. Each line is in the form: PageName	PageId
public class Bz2WikiParserMR {

	// Maintain a global counter for counting page names 
	public static enum UpdateCounter{
		PAGENAMECOUNT;
	}

	// Configured Bz2Parsing Job
	public static Job runMRBz2Parsing(String[] args, Configuration conf) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		conf.set("directory", args[1]);
		Job job = Job.getInstance(conf, "Bz2 Parser");
		job.setJarByClass(Bz2WikiParserMR.class);
		job.setMapperClass(Bz2Mapper.class);
		job.setReducerClass(Bz2Reducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		
		// Multiple Output Configuration for producing four files as explained
		MultipleOutputs.addNamedOutput(job, "OutputM", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "OutputD", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "OutputR", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "OutputN", TextOutputFormat.class, Text.class, Text.class);
		
		job.waitForCompletion(true);

		return job;

	}
	
}
