package cs6240;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SampleSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (myArgs.length < 2) {
            System.err.println("Usage: hadoop jar this.jar <in> <out>");
            System.exit(2);
        }
        
        FileSystem fs = FileSystem.get(conf);

        fs.delete(new Path("samples"), true);
        runSample(conf, myArgs[0], "samples", 9);

        fs.delete(new Path(myArgs[1]), true);
        runSort(conf, myArgs[0], myArgs[1], "samples/part-r-00000");
        
        System.exit(0);
    }

    static void runSample(Configuration conf, String input, String output, int quants) throws Exception {
    	conf.set("filter_percentage", "0.05"); // 1/20
    	conf.set("num-quants", "" + quants);
        Job job = new Job(conf, "sampler");
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(QuantReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    static void runSort(Configuration conf,
            String input, String output, String samps) throws Exception {
        conf.set("samps", samps);
        Job job = new Job(conf, "sampler");
        // Must use 10 reduce tasks
        job.setNumReduceTasks(10);
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(SortPartitioner.class);
        job.setReducerClass(Reducer.class);
     
        job.setOutputKeyClass(Text.class);
      	job.setOutputValueClass(NullWritable.class);
        
    	FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
      	
      	
      	// Reducer is an identity reducer
        // FIXME

        job.waitForCompletion(true);
    }
}


