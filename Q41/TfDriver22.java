package tfidf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tfidf.Map1;
import tfidf.Reduce1;
import tfidf.Map2;
import tfidf.Reduce2;
import tfidf.Map3;
import tfidf.Reduce3;


public class TfDriver22 extends Configured implements Tool { 
	
	public static void main(String[] argumente) throws Exception {
	      System.out.println(Arrays.toString(argumente));
	      int result = ToolRunner.run(new Configuration(), new TfDriver22(), argumente);
	      
	      System.exit(result);
	   }
	
	
	@Override
	   public int run(String[] argumente) throws Exception {
	      System.out.println(Arrays.toString(argumente));
	      
	      //job 1
	      Job job1 = Job.getInstance(new Configuration(), "Job #1");
	      job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); 
	      job1.setJarByClass(TfDriver22.class);
	      
	      job1.setOutputKeyClass(Text.class);
	      job1.setOutputValueClass(Text.class);
	      
	      job1.setMapperClass(Map1.class);
	      job1.setReducerClass(Reduce1.class);

	      job1.setInputFormatClass(TextInputFormat.class);
	      job1.setOutputFormatClass(TextOutputFormat.class);

	      Path outputPath = new Path(argumente[1]);
	      FileSystem hdfs = FileSystem.get(getConf());
	      if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		  }
		  
	      FileInputFormat.addInputPath(job1, new Path(argumente[0]));
	      FileOutputFormat.setOutputPath(job1, new Path(argumente[1]+"/Interim1"));

	      job1.waitForCompletion(true);
	      
	      System.out.println("Job 1 finished.");
	      
	      //Job 2
	      Job job2 = Job.getInstance(new Configuration(), "Job #2");
	      job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); 
	      job2.setJarByClass(TfDriver22.class);
	      
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);
	      
	      job2.setMapperClass(Map2.class);
	      job2.setReducerClass(Reduce2.class);

	      job2.setInputFormatClass(TextInputFormat.class);
	      job2.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job2, new Path(argumente[1]+"/Interim1"));
	      FileOutputFormat.setOutputPath(job2, new Path(argumente[1]+"/Interim2"));

	      job2.waitForCompletion(true);
	      System.out.println("Job 2 finished.");
	      
	      
	      //Job 3
	      Job job3 = Job.getInstance(new Configuration(), "Job #3");
	      job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";"); 
	      job3.setJarByClass(TfDriver22.class);
	      
	      job3.setOutputKeyClass(Text.class);
	      job3.setOutputValueClass(Text.class);
	      
	      job3.setMapperClass(Map3.class);
	      job3.setReducerClass(Reduce3.class);

	      job3.setInputFormatClass(TextInputFormat.class);
	      job3.setOutputFormatClass(TextOutputFormat.class);
	      
	      FileInputFormat.addInputPath(job3, new Path(argumente[1]+"/Interim2"));
	      FileOutputFormat.setOutputPath(job3, new Path(argumente[1]+"/Final"));

	      job3.waitForCompletion(true);
	      System.out.println("Job 3 finished.");
	      
	      //Sort files
	      BufferedReader reader = new BufferedReader(new FileReader(argumente[1]+"/Final/part-r-00000"));

	      ArrayList<WordScore> tfidf_score = new ArrayList<WordScore>();

	      String currentLine = reader.readLine();
	         
	      while (currentLine != null)
	      	{
	           	String[] WordScoreDetail = currentLine.split(";");
	            String name = (WordScoreDetail[0]+";" + WordScoreDetail[1]);
	            Float scores = Float.valueOf(WordScoreDetail[2]);  
	            tfidf_score.add(new WordScore(name, scores));
	            currentLine = reader.readLine();
	        }

	      Collections.sort(tfidf_score, new scoresCompare());

	      BufferedWriter writer = new BufferedWriter(new FileWriter(argumente[1]+"/Final/part-r-00000-sorted"));
	      for (WordScore student : tfidf_score) 
	        {
	            writer.write(student.name);
	            writer.write(";"+student.scores);
	            writer.newLine();
	        }
	        
	      reader.close();
	      writer.close();
	        
	      System.out.println("Sorting finished.");
	      
	      return 0;

	   }
	 
	class WordScore
	{
	    String name;
	    float scores;
	     
	    public WordScore(String name, float scores) 
	    {
	        this.name = name;
	        this.scores = scores;
	    }
	}
	 
	 
	class nameCompare implements Comparator<WordScore>
	{
	    @Override
	    public int compare(WordScore s1, WordScore s2)
	    {
	        return s1.name.compareTo(s2.name);
	    }
	}
	 
	 
	class scoresCompare implements Comparator<WordScore>
	{
	    @Override
	    public int compare(WordScore s1, WordScore s2)
	    {	
	    	int aux= 1;
	    	if (s2.scores <= s1.scores){
	    		aux= -1;
	    	}
	    	
	    return aux;
	    }
	}
}

