package mapperClasses;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, ArrayWritable, Text, Text>{
	
	public void map(LongWritable key, ArrayWritable value, Context context) throws IOException, InterruptedException{
		Text reduceKey = new Text();
		Text reduceValue = new Text();
		
		Text[] texts = (Text[]) value.get();
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
		
		
	}

}
