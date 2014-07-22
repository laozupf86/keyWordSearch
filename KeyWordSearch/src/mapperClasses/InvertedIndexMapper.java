package mapperClasses;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, ArrayWritable, Text, Text>{
	
	
	
	public void map(LongWritable key, ArrayWritable value, Context context) throws IOException, InterruptedException{
		Text reduceKey = new Text();
		Text reduceValue = new Text();
		System.out.println("into map");
		Text[] texts = (Text[]) value.get();
		String hostname = java.net.InetAddress.getLocalHost().getHostName();
		System.out.println("host name is " + hostname);
		//split the point and keywords
		for(Text text : texts){
			String line = text.toString();
			String parts[] = line.split("@");
			String words[] = parts[parts.length - 1].split(",");
			reduceValue.set(parts[0]);
			for(String word : words){
				reduceKey.set(word);
				context.write(reduceKey, reduceValue);
			}
		}
		FileSplit file = (FileSplit) context.getInputSplit();
		System.out.println("current file location is in " + file.getPath() + " has " + file.getLocations().length + " locations");
		
		
		
		
	}
	
	

}
