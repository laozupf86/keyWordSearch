package reducerClasses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;









import dataStructureClasses.Point;
import etcClasses.DistanceCalculation;

public class SearchReducer extends Reducer<Point, Text, Point, DoubleWritable>{
	
	public Map<Text, Integer> keywords = new HashMap<>();
	public double bestSoFarDistance = Double.POSITIVE_INFINITY;
	public Point queryPoint = null;
	public DoubleWritable distanceToWrite = new DoubleWritable();
	
	/**
	 * 
	 * @param point POI
	 * @param values keywords
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void reduce(Point point, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException{
		
		//List<Text> pointKeyWords = new ArrayList<>();
		for(ArrayWritable value : values){
			Text[] words = (Text[]) value.get();
			for(Text word : words){
				if(!keywords.containsKey(word)){
					return;
				}
				//pointKeyWords.add(word);
			}
		}
		double distance = DistanceCalculation.calculateDistance(queryPoint, point);
		if(distance < bestSoFarDistance){
			bestSoFarDistance = distance;
			distanceToWrite.set(distance);
			context.write(point, distanceToWrite);
			
		}
		
		
		
		
	}
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{
		
		String word = context.getConfiguration().get("keyword");
		String[] words = word.split(",");
		for(String w : words){
			Text text = new Text(w);
			keywords.put(text, 1);
		}
		String point = context.getConfiguration().get("point");
		String[] p = point.split(",");
		queryPoint = new Point(-1, Float.parseFloat(p[0]), Float.parseFloat(p[1]));
		
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		
		super.cleanup(context);
	}

}
