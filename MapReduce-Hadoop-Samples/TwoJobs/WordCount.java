package TwoJobs;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import WordCount.WordCount.IntSumReducer;
import WordCount.WordCount.TokenizerMapper;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
      
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
         
        Job job1 = new Job(conf, "word count");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapperWordCount.class);
        job1.setReducerClass(IntSumReducerWordCount.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }

        Path temp = new Path("./temp-data");
        FileOutputFormat.setOutputPath(job1,temp);
        
        job1.waitForCompletion(true);
        
        
        Job job2 = new Job(new Configuration(),"reading job");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(TokenizerMapper.class);
        job2.setPartitionerClass(SortPartitioner.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setNumReduceTasks(2);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2,temp);
        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1]));
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("temp"), true);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
          
    
    }

public static class TokenizerMapper extends Mapper<Object, Text,IntWritable,Text>{
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            String k = itr.nextToken();
            Integer v = Integer.parseInt(itr.nextToken());
            context.write(new IntWritable(v), new Text(k));
            
        }
    }
}

public static class SortPartitioner extends Partitioner<IntWritable, Text> {
    @Override
    public int getPartition(IntWritable key, Text value, int nrt) {
    	//System.out.println(nrt);
    	if(key.get() < 10)
    		return 0;
    	else
    		return 1;
    }
}

public static class IntSumReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
   
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(key, val);
        }
        
    }
}

public static class TokenizerMapperWordCount extends Mapper<Object, Text, Text, IntWritable>{
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            Matcher mm1 = nw1.matcher(itr.nextToken());
            Matcher mm2 = nw2.matcher(mm1.replaceAll("")); 
            String ww = mm2.replaceAll("").toLowerCase();

            if (!ww.equals("")) {
                word.set(ww);
                context.write(word, one);
            }
        }
    }
}
 
public static class IntSumReducerWordCount extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}


}


