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
			System.out.println("key is " + key.toString() + " value is " + value.toString());
			sb.append(value.toString());
			sb.append(linebreaker);
		}
		System.out.println("end reduce");
		Text newValue = new Text();
		newValue.set(sb.toString());
		mo.write(key, newValue, key.toString());
		String hostname = java.net.InetAddress.getLocalHost().getHostName();
		System.out.println("host name is " + hostname);
		//context.write(key, newValue);
		
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
