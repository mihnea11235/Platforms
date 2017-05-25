package hadoop.pagerank;

import hadoop.pagerank.PageRank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

//include jobs
import hadoop.pagerank.Job1Mapper;
import hadoop.pagerank.Job1Reducer;
import hadoop.pagerank.Job2Mapper;
import hadoop.pagerank.Job2Reducer;
import hadoop.pagerank.Job3Mapper;

public class PageRank {
    
	//attributes
    public static NumberFormat nr_format = new DecimalFormat("00");
    public static Set<String> noduri = new HashSet<String>();
    public static String sep_link = ",";
	
    // configuration
    public static Double DAMPING = 0.85;
    public static int nr_runs = 3;

    //main class
    public static void main(String[] args) throws Exception {
        
        String input_path = args[0];
        String output_path = args[1];
    	
    	String inPath = null;;
        String lastOutPath = null;
        PageRank pagerank = new PageRank();
        
        //Prep data
        System.out.println("Job 1: Data preparation");
        
	    Path outputPath = new Path(output_path);
	    FileSystem hdfs = FileSystem.get(new Configuration());
	    if (hdfs.exists(outputPath)){
		      hdfs.delete(outputPath, true);
		}
        
        pagerank.job1(input_path, output_path + "/iter00");
        
        //Get probabilities
        for (int runs = 0; runs < nr_runs; runs++) {
            inPath = output_path + "/iter" + nr_format.format(runs);
            lastOutPath = output_path + "/iter" + nr_format.format(runs + 1);
            System.out.println("Job 2: " + (runs + 1) + "/" + PageRank.nr_runs + " computed PageRank");
            pagerank.job2(inPath, lastOutPath);
        }
        
        //Map them togethere
        System.out.println("Job 3: Ordered Rank");
        pagerank.job3(lastOutPath, output_path + "/result");
        
    }


    public boolean job1(String in, String out) throws IOException, 
    ClassNotFoundException, 
    InterruptedException {

    	Job job = Job.getInstance(new Configuration(), "Job #1");
    	job.setJarByClass(PageRank.class);

    	FileInputFormat.addInputPath(job, new Path(in));
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setMapperClass(Job1Mapper.class);

    	FileOutputFormat.setOutputPath(job, new Path(out));
    	job.setOutputFormatClass(TextOutputFormat.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setReducerClass(Job1Reducer.class);

    	return job.waitForCompletion(true);

    }
    
    
    public boolean job2(String in, String out) throws IOException, 
    ClassNotFoundException, 
    InterruptedException {

    	Job job = Job.getInstance(new Configuration(), "Job #2");
    	job.setJarByClass(PageRank.class);

    	FileInputFormat.setInputPaths(job, new Path(in));
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setMapperClass(Job2Mapper.class);

    	FileOutputFormat.setOutputPath(job, new Path(out));
    	job.setOutputFormatClass(TextOutputFormat.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	job.setReducerClass(Job2Reducer.class);

    	return job.waitForCompletion(true);

    }


    public boolean job3(String in, String out) throws IOException, 
    ClassNotFoundException, 
    InterruptedException {

    	Job job = Job.getInstance(new Configuration(), "Job #3");
    	job.setJarByClass(PageRank.class);

    	FileInputFormat.setInputPaths(job, new Path(in));
    	job.setInputFormatClass(TextInputFormat.class);
    	job.setMapOutputKeyClass(DoubleWritable.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setMapperClass(Job3Mapper.class);

    	FileOutputFormat.setOutputPath(job, new Path(out));
    	job.setOutputFormatClass(TextOutputFormat.class);
    	job.setOutputKeyClass(DoubleWritable.class);
    	job.setOutputValueClass(Text.class);

    	return job.waitForCompletion(true);

}

}
