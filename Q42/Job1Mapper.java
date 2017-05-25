package hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        if (value.charAt(0) != '#') {
            
            int Tind = value.find("\t");
            String node1 = Text.decode(value.getBytes(), 0, Tind);
            String node2 = Text.decode(value.getBytes(), Tind + 1, value.getLength() - (Tind + 1));
            context.write(new Text(node1), new Text(node2));

            PageRank.noduri.add(node1);

            PageRank.noduri.add(node2);
            
        }
 
    }
    
}
