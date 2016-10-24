package com.mapreduce.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.mapreduce.pagerank.IteratePageRank;
import com.mapreduce.parser.Bz2WikiParserMR;
import com.mapreduce.top100.Top100Records;

// Main Driver for the all the jobs
public class ChainingJobs {

	// Number of Iterations of PageRank Job
	public static final int ITERATIONS = 10;

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		// Common configuration which is passed to all the map reduce jobs 
		// To access/update pageNameCount and danglingOffset variables
		Configuration conf = new Configuration();

		// Run the Bz2WikiParserMR and fetch the initial PAGENAMECOUNT counter
		// Set the PAGENAMECOUNT counter to pageNameCount variable of configuration
		String[] bz2Args = {args[0],args[1]};
		Job bz2 = Bz2WikiParserMR.runMRBz2Parsing(bz2Args,conf);
		conf.set("pageNameCount", Double.toString(bz2.getCounters().findCounter(Bz2WikiParserMR.UpdateCounter.PAGENAMECOUNT).getValue()));


		String input = args[2] + "0";
		String output = args[2] + "1";
		String[] pageRankArgs = {input,output};

		// Initailize danglingOffset as 0.0
		Double danglingOffset = 0.0;

		for(int i = 1; i <= ITERATIONS ; i++){
			// Update danglingOffset with the danglingOffset value in configuration
			conf.set("danglingOffset", Double.toString(danglingOffset));
			// Execute PageRank Job
			Job job = IteratePageRank.runIteratePageRank(pageRankArgs, conf);
			// Fetch the latest value of DANGLINGOFFSET from the last job of IteratePageRank
			long l =  job.getCounters().findCounter(IteratePageRank.UpdateCounter.DANGLINGOFFSET).getValue();
			// Extract Precision Values from Dangling Offset δ
			danglingOffset = (double) l/1000000000; // convert to double
			// by the formula calculate δ / |V|
			danglingOffset = (double)danglingOffset / Double.parseDouble(conf.get("pageNameCount"));

			// Update the pageNameCount in configuration by fetching value of PAGENAMECOUNTER from the last job
			conf.set("pageNameCount", Long.toString(job.getCounters().findCounter(IteratePageRank.UpdatePageNameCounter.PAGENAMECOUNTER).getValue()));

			if(i!=ITERATIONS){
				input = output;
				output = output.substring(0, output.length()-1) + Integer.toString(i+1);
				pageRankArgs[0] = input;
				pageRankArgs[1] = output;
			}
		}

		String[] finalArgs = {output,output.substring(0, output.length()-2) + "Top100"};
		Top100Records.fetchTop100FrequentRecords(finalArgs);

	}	


}
