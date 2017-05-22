package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text FileName = new Text();


   @Override
   public void map(LongWritable key, Text value, Context context)
           throws IOException, InterruptedException {
  	 
       for (String token: value.toString().replaceAll("[^0-9A-Za-z]"," ").split("\\s+")){
         	String name_file = ((FileSplit) context.getInputSplit()).getPath().getName();
         	FileName = new Text(name_file);
         	token = token.toLowerCase();
         	word.set(token);
         	if(!word.equals("") && word.getLength()!=0){
         		context.write(word, FileName);
         	}
          }
     }
 }
