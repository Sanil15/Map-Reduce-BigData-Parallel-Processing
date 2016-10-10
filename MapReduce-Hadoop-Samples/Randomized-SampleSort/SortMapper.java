package cs6240;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class SortMapper extends Mapper<Object, Text, Text, NullWritable> {
    NullWritable nw = NullWritable.get();

    public void map(Object key, Text line,Context context) throws IOException, InterruptedException {
        context.write(line, nw);
    }
}
 
