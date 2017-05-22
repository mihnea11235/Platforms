package tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce3 extends Reducer<Text, Text, Text, Text> {
	 
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)
             throws IOException, InterruptedException {
		 HashSet<String> hshst = new HashSet<String>();
		 Double AllDocs = 0.00;
		 String word = key.toString();
		 String doc = new String();
		 Double WrdCntDoc = 0.00;
		 Integer frequency = 0;
		 Integer DocWord = 0;
		 Double tf_idf = 0.00;
		 ArrayList<String> cache = new ArrayList<String>();
		 
		 for (Text value : values){
			 doc = value.toString().split(",")[0];
			 DocWord++;
			 cache.add(value.toString());
			 if(!hshst.contains(doc)){
	    		 hshst.add(doc);
	    		 AllDocs++;
	    	 }
		 }
		 
		 for (String val : cache){
			 doc = val.split(",")[0];
			 frequency = Integer.parseInt(val.split(",")[1]);
			 WrdCntDoc = Double.parseDouble(val.split(",")[2]);
			 
			 tf_idf = (frequency/WrdCntDoc)+Math.log(AllDocs/DocWord);
			 
			 DecimalFormat df = new DecimalFormat("#.##########");

			 //System.out.println(frequency);
			 context.write(new Text(word+";"+doc), new Text(df.format(tf_idf).toString()));
		 }
		 
		 
		 
	 }
}

