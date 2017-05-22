package tfidf;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {
	 
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)
             throws IOException, InterruptedException {
	
		 String doc = key.toString();
		 Integer WrdCntDoc = 0;
		 Integer frequency = 0;
		 String word = new String();
		 ArrayList<String> cache = new ArrayList<String>();
		 for (Text value : values){
			 frequency = Integer.parseInt(value.toString().split(";")[1]);
			 cache.add(value.toString());
			 WrdCntDoc+=frequency;
		 }
		 
		 for (String val : cache){
			 word = val.split(";")[0];
			 frequency = Integer.parseInt(val.split(";")[1]);
			 context.write(new Text(word+","+doc), new Text(frequency+","+WrdCntDoc));
		 }
     }
}
