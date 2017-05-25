package hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
        String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
        
        String[] toate_pg = links.split(",");
        for (String alta_pg : toate_pg) { 
            Text rank_total_pagina = new Text(pageRank + "\t" + toate_pg.length);
            context.write(new Text(alta_pg), rank_total_pagina); 
        }
        context.write(new Text(page), new Text(PageRank.sep_link + links));
        
    }
    
}

