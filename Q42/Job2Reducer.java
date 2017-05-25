package hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        String links = "";
        double suma_pagini = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith(PageRank.sep_link)) {
 
                links += content.substring(PageRank.sep_link.length());
            } else {
                
                String[] split = content.split("\\t");
 
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);

                suma_pagini += (pageRank / totalLinks);
            }

        }
        
        double newRank = PageRank.DAMPING * suma_pagini + (1 - PageRank.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
        
    }

}
