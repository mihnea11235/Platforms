package hadoop.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int idx_1 = value.find("\t");
        int idx_2 = value.find("\t", idx_1 + 1);

        String page = Text.decode(value.getBytes(), 0, idx_1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), idx_1 + 1, idx_2 - (idx_1 + 1)));
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}
