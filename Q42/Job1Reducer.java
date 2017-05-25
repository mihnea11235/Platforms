package hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        boolean primul = true;
        String legaturi = (1 / PageRank.noduri.size()) + "\t";

        for (Text value : values) {
            if (!primul) 
                legaturi += ",";
            legaturi += value.toString();
            primul = false;
        }

        context.write(key, new Text(legaturi));
    }

}
