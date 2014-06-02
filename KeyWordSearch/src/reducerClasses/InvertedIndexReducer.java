package reducerClasses;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
	
	private MultipleOutputs<Text, Text> mo;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		StringBuilder sb =  new StringBuilder();
		String linebreaker = System.getProperty("line.separator");
		for(Text value : values){
			sb.append(value.toString());
			sb.append(linebreaker);
		}
		Text newValue = new Text();
		newValue.set(sb.toString());
		mo.write(key, newValue, key.toString());
		
		
	}
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{
		mo = new MultipleOutputs<Text, Text>(context);
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		mo.close();
		super.cleanup(context);
	}

}
