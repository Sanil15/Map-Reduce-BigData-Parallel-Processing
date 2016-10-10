package cs6240;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class SortPartitioner extends Partitioner<Text,NullWritable> implements Configurable {
    Configuration conf;
    public static ArrayList<String> splitPoints = new ArrayList<>();
    public void setup() { // FIXME
        BufferedReader rdr;
        try {
            // FIXME
        	String sCurrentLine;
        	String sampPath = getConf().get("samps");
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream ss = fs.open(new Path(sampPath));
            rdr = new BufferedReader(new InputStreamReader(ss));
            while ((sCurrentLine = rdr.readLine()) != null) {
				splitPoints.add(sCurrentLine);
			}
            
          
        }
        catch (Exception ee) {
            throw new Error(ee.toString());
        }
    }

    public int getPartition(Text key, NullWritable value, int np) {
        int i=0;
    	while(i<splitPoints.size() && key.toString().compareTo(splitPoints.get(i))<0)
    		i++;
    	if(i!=0)
    	return i-1;
    	
    	return i;
    }

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		this.conf = arg0;
		setup();
	}
}
