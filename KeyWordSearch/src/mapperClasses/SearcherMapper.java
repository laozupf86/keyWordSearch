package mapperClasses;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import dataStructureClasses.Point;



public class SearcherMapper extends Mapper<Text, ArrayWritable, Point, Text>{

	int k = 1;
	
	/**
	 * map the <K1, V1> (<keyword, points>) to <K2, V2> (<pointid, keyword>)
	 */
	public void map(Text key, ArrayWritable value, Context context) throws IOException, InterruptedException{
		
		Point[] points = (Point[]) value.get();
		for(Point point : points){
			context.write(point, key);
		}
		
		
		
		
	}
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		
		k = Integer.parseInt(conf.get("k"));
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		
		super.cleanup(context);
	}

	
}
