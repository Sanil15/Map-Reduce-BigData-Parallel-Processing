package com.mapreduce.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// Node class implements Writable interface since it is emitted as a key
// from the Mapper 
public class Node implements Writable {

	// Represents the unique page name
	private Text pageName = new Text();
	// Represents the page rank of the page
	private DoubleWritable pageRank = new DoubleWritable();
	// Represents all the outlinks from the page
	private ArrayList<Text> adjacencyList = new ArrayList<>();
	
	public Node(){
	}
	
	public Node(Text pageName, DoubleWritable pageRank) {
		this.pageName = pageName;
		this.pageRank = pageRank;
	}


	public Text getPageName() {
		return pageName;
	}

	public void setPageName(Text pageName) {
		this.pageName = pageName;
	}

	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	public ArrayList<Text> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(ArrayList<Text> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}
	
	
	public void fillAdjacencyList(String[] outlinkList){
		for(String outlink: outlinkList){
			adjacencyList.add(new Text(outlink));
		}
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		pageName.readFields(in);
		// special serialization for adjacency list, read the
		// size first.
		int size = in.readInt();
        adjacencyList = new ArrayList<>(size);
        while(size-- > 0) {
            Text a = new Text();
            a.readFields(in);
            adjacencyList.add(new Text(a));
        }
        pageRank.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		pageName.write(out);
		// Write the size of adjacency list first
		out.writeInt(adjacencyList.size());
		for (Text a : adjacencyList)
            a.write(out);
		pageRank.write(out);
	}

	
	@Override
	public int hashCode(){
		return pageName.toString().hashCode();
	}

	@Override
	public String toString() {
		return pageRank + "\t" + adjacencyList;
	}

	
}
