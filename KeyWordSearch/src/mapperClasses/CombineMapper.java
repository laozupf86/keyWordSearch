package mapperClasses;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class CombineMapper extends Mapper<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
	
	
	public void map(IntWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException{
		
		context.write(key, value);
		
		
	}

}
