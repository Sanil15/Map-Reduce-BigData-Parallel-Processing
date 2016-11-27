package com.mapreduce.driver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.mapreduce.dangling.DanglingDriver;
import com.mapreduce.pagerank.columnbyrow.DotProductDriver;
import com.mapreduce.pagerank.columnbyrow.PageRankDriver;
import com.mapreduce.pagerank.rowbycolumn.PageRankDriverRowByColumn;
import com.mapreduce.parser.Bz2WikiParserMR;
import com.mapreduce.top100.Top100Records;

// Main Driver for the all the jobs
public class ChainingJobs {

	// Number of Iterations of PageRank Job
	public static final int ITERATIONS = 10;

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
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
		conf.set("pageNameCount", Long.toString(bz2.getCounters().findCounter(Bz2WikiParserMR.UpdateCounter.PAGENAMECOUNT).getValue()));
		
		String basePath = args[1];
		String dummyPath = args[3];
		String pagePath = args[4];
		String config = args[5];

		// Run COLUMN * ROW VERSION
		if(config.equals("COLUMN")){

			for(int i=0; i< ITERATIONS; i++){
				if(i==0){
					// 𝐌′𝐑(𝑡) = (𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡).
					
					// Calculation of DR(0)
					String[] danglingArgs = {basePath+"/OutputD-r-00000",basePath+"/OutputR-r-00000",dummyPath+"/"+i};
					Job dangling = DanglingDriver.runDanglingCalculation(danglingArgs, conf);
					// Update the dangling counter for each iteration
					conf.setLong("danglingOffset", dangling.getCounters().findCounter(DanglingDriver.UpdateCounter.DANGLINGOFFSET).getValue());
					
					// Single Page Rank Configured as two Jobs: PageRankDriver & DotProductDriver 
					// Calculation of 𝐌'𝐑(𝑡) 
					String[] pageRankArgs = {basePath+"/OutputM-r-00000",basePath+"/OutputR-r-00000",pagePath+"/DUMR("+i+")"};
					Job pageRank = PageRankDriver.runPageRank(pageRankArgs, conf);
					String[] dotProduct ={pagePath+"/DUMR("+i+")",pagePath+"/R("+i+")"};
					Job dRank = DotProductDriver.runDotProduct(dotProduct, conf);

				}else{

					// 𝐌′𝐑(𝑡) = (𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡).
					
					// Calculation of DR(t)
					String[] danglingArgs = {basePath+"/OutputD-r-00000",pagePath+"/R("+(i-1)+")",dummyPath+"/"+i};
					// Update the dangling counter for each iteration
					Job dangling = DanglingDriver.runDanglingCalculation(danglingArgs, conf);
					conf.setLong("danglingOffset", dangling.getCounters().findCounter(DanglingDriver.UpdateCounter.DANGLINGOFFSET).getValue());
					
					// Single Page Rank Configured as two Jobs: PageRankDriver & DotProductDriver 
					// Calculation of 𝐌'𝐑(𝑡) 
					String[] pageRankArgs = {basePath+"/OutputM-r-00000",pagePath+"/R("+(i-1)+")",pagePath+"/DUMR("+i+")"};
					Job pageRank = PageRankDriver.runPageRank(pageRankArgs, conf);
					String[] dotProduct ={pagePath+"/DUMR("+i+")",pagePath+"/R("+i+")"};
					Job dRank = DotProductDriver.runDotProduct(dotProduct, conf);
				}

			}
		}
		// RUN ROW * COLUMN VERSION
		else{
			for(int i=0; i< ITERATIONS; i++){
				if(i==0){
				
					// 𝐌′𝐑(𝑡) = (𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡).
					
					// Calculation of DR(0)
					String[] danglingArgs = {basePath+"/OutputD-r-00000",basePath+"/OutputR-r-00000",dummyPath+"/"+i};
					Job dangling = DanglingDriver.runDanglingCalculation(danglingArgs, conf);
					// Update the dangling counter for each iteration 
					conf.setLong("danglingOffset", dangling.getCounters().findCounter(DanglingDriver.UpdateCounter.DANGLINGOFFSET).getValue());
				
					
					// Single Page Rank Configured as a Job: PageRankDriver 
					// Calculation of 𝐌'𝐑(𝑡) 
					String[] pageRankArgs = {basePath+"/OutputM-r-00000",pagePath+"/R("+i+")"};
					String cache = basePath+"/OutputR-r-00000";
					Job pageRank = PageRankDriverRowByColumn.runPageRank(pageRankArgs,cache,conf);

				}else{

					// 𝐌′𝐑(𝑡) = (𝐌 + 𝐃)𝐑(𝑡) = 𝐌𝐑(𝑡) + 𝐃𝐑(𝑡).
					
					// Calculation of DR(t)
					String[] danglingArgs = {basePath+"/OutputD-r-00000",pagePath+"/R("+(i-1)+")",dummyPath+"/"+i};
					Job dangling = DanglingDriver.runDanglingCalculation(danglingArgs, conf);
					// Update the dangling counter for each iteration 
					conf.setLong("danglingOffset", dangling.getCounters().findCounter(DanglingDriver.UpdateCounter.DANGLINGOFFSET).getValue());
					
					// Calculation of 𝐌'𝐑(𝑡) 
					// Single Page Rank Configured as a Job: PageRankDriver 
					String[] pageRankArgs = {basePath+"/OutputM-r-00000",pagePath+"/R("+i+")"};
					String cache = pagePath+"/R("+(i-1)+")";
					Job pageRank = PageRankDriverRowByColumn.runPageRank(pageRankArgs,cache, conf);
										
				}

			}
		}

		// Top100 Job for finding top100 names with page ranks
		String[] top100Args = {pagePath+"/R("+(ITERATIONS-1)+")",basePath+"/OutputN-r-00000",args[2]};
		Top100Records.fetchTop100FrequentRecords(top100Args);

	}	

}
