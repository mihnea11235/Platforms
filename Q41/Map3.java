package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map3 extends Mapper<LongWritable, Text, Text, Text> {
	 
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
   	 
   	 String word = value.toString().split(";")[0].split(",")[0];
   	 String doc = value.toString().split(";")[0].split(",")[1];
   	 String frequency = value.toString().split(";")[1].split(",")[0];
   	 String WrdCntDoc = value.toString().split(";")[1].split(",")[1];
   	 
   	 context.write(new Text(word), new Text(doc+","+frequency+","+WrdCntDoc));
      }
  }
