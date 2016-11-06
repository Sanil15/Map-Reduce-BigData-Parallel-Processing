package com.mapreduce.main;
import com.mapreduce.parser._;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Driver {
	def main(args: Array[String]) {

		try{
			println("Driver: startup")

			// Spark Configuration 
			val sparkConf = new SparkConf().
			setAppName("Page Rank Spark").
			setMaster(args(2)) // This is "local[*]" for local execution and "yarn" for EMR
			val sc = new SparkContext(sparkConf);

			// Step 1 
			// This step just parses each line via Bz2Parser and then creates an
			// RDD[(String, (Double, List[String]))] ~ RDD[(pageName, (pageRank, adjacencyList))]
			// It also filters any invalid node i.e containing invalid page names. Note that this 
			// node RDD has to be persisted for optimization due to Lazy loading by Spark 
			var nodes = sc.textFile(args(0)).
					map{line => Bz2WikiParser.parseLine(line).split("\t")}. // Parse Ever Line with Bz2WikiParser
					filter { row => (row(0) != null && row(0).trim() != "")}. // Filter Pages with invalid names
					map{row => {
						try{
							var pageName = row(0).trim(); 
							var pageRank = row(1).toDouble;
							var adjacencyList = List[String]();
							if(row.length > 2)
								adjacencyList = row(2).split("~").toList;
							(pageName,pageRank,adjacencyList)
						}catch{
						case e: Throwable => ("",0.0d,List())}}}
					.keyBy(row => (row._1)) // Make Key as page name
					.persist() // Persist RDD due to Lazy loading in Spark
					.mapValues(row => (row._2,row._3));	// Remove pageName from second tuple as key is page name	

				// Step 2 
				// This Step first combines all nodes in an all the adjacency list of all the nodes in the graph.
				// Creates an RDD and makes a union with completeGraph RDD. Then a reduce by key which just merges the 
				// adjacency list of elements in RDD with same Keys. Note. If this step not done, node count will decrease after first page rank 
				// Combine all the adjacency list of all the nodes in the complete graph
				var combinedAdjacencyLists = nodes.flatMap( row => (row._2._2)); 
				// Iterate over combinedAdjacencyLists to make an RDD RDD[(String, (Double, List[String]))] ~ RDD[(pageName, (pageRank, adjacencyList))]
				// Note. All the nodes here are initialized with garbage page rank value and an empty adjacency list
				var adjacencyListNodesRDD = combinedAdjacencyLists.map(row => (row,0.0,List[String]()))
						                        .keyBy(row => row._1)
						                        .mapValues(row => (row._2,row._3));				
				// Take a union of adjacencyListNodesRDD and completeGraph RDD 
				// Then perform a reduce by key with operation to merge adjacency list of elements in RDD with same page names i.e keys
				var completeGraph = nodes.union(adjacencyListNodesRDD)
				                    .reduceByKey((p,q) =>(p._1,(p._2++q._2)))

				
				// Step 3 
				// This step just puts a mapValues over completeGraph RDD to initialize page rank values 1/N 
				// where N is the total number of nodes in graph (count of completeGraph) and make a new RDD with 
				// RDD[(String, (Double, List[String]))] ~ RDD[(pageName, (pageRank, adjacencyList))] called graphComplete
				var totalNodes = completeGraph.count;
				var initilVal = (1.0 / totalNodes);
				var graphComplete = completeGraph.mapValues(row => (initilVal,row._2));	


			  // Step 4
		    // Iteration for page rank
			  // Counter needs to be initialized for every iteration to keep dangling offset value 
				var i = 1;
				for(i<- 1 to 10){

					  // Use a double accumulator as counter for dangling nodes
						val danglingOffsetCounter= sc.doubleAccumulator("dangling");

						// Step 5 
						// This step maps over the graphComplete just extracts page rank and adjacency list from each node
						// Identifies dangling node, if found updates the dangling counter. Otherwise distributes page rank across 
						// all the nodes in the adjacency list (outlinks) and creates a RDD with 
						// RDD[(String, Double)] ~ RDD[(outlink, contribution)] called contributionGraph 
						var contributionGraph = graphComplete.map(row => (row._2._1,row._2._2)).
								flatMap{ row => {
									try{
									  // Check Dangling, if found update counter 
										if(row._2.length == 0){
											danglingOffsetCounter.add(row._1); 
											List()
										}
										// Otherwise, Distribute page rank across all outlinks equally
										else{
											var contribution = row._1 / row._2.length;    	
											row._2.map{outlink => (outlink.trim(), contribution)}
										}}catch{
										case e: Throwable => (List())}		
								}}.keyBy(row => (row._1)).mapValues(row => (row._2));

						// Note: This is reduceByKey to add page rank contributions across multiple elements with same key as pageName
						// This step just improves the efficiency 		
						contributionGraph = contributionGraph.reduceByKey((p,q) => (p+q));

						
						// Step 6
						// Join the graphComplete with contributionGraph, now extract sum of contributions from inlinks to a 
						// page from contributionGraph RDD to calculate a new page rank value and now create a new RDD
					  // RDD[(String, (Double, List[String]))] ~ RDD[(pageName, (pageRank, adjacencyList))]
					  // update graphComplete to this new RDD so that updated values are read in next iterations 
						var graphStruct = graphComplete.join(contributionGraph); // Perform inner join on page names
						totalNodes = graphStruct.count();
						graphComplete = graphStruct.map{ row => { 
						try{
								// Calculate new page rank value 
								var pageRank = (0.15/totalNodes)+((0.85)*(row._2._2 + danglingOffsetCounter.value));
								((row._1),pageRank,(row._2._1._2))
							 }catch{
								case e: Throwable => ("",0.0d,List())}}}
						.keyBy(row => row._1)
						.mapValues(row =>(row._2,row._3));


					}

					// Step 7
					// Create a new RDD top100 with pageName and pageRank from graphComplete
				  // Take top 100 from every partition in descending order of page rank
				  // then make one partition and sort to fetch a global top 100 in descending order of page rank
					var top100 = graphComplete.map(row => (row._1,row._2._1));
					var output = top100.takeOrdered(100)(Ordering[Double].reverse.on { x => x._2 }); // Takes top 100 from all the partitions
					var top100Sorted = sc.parallelize(output); // Convert to an RDD again
					top100Sorted = top100Sorted.repartition(1); // Just Create a single partition
					top100Sorted = top100Sorted.sortBy(-_._2); // Sort on decreasing order of page ranks
					top100Sorted.saveAsTextFile(args(1));
				    

		}catch{
		case e: Throwable => 
		}

	}
}
