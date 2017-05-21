package LC;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class count {

	public static void main(String[] args) throws IOException {

		Path filename = new Path(args[0]);
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		int count = 0;
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			String cvsSplitBy = ";";
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				String[] data = line.split(cvsSplitBy);
				System.out.println(data[5] + " " + data[6]+ "\n\n");
				// add line to count
				count +=1;
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
			System.out.println("Nr of lines= "+ count);
		}

		
		
	}

}
