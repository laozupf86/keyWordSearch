package reducerClasses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineReducer extends Reducer<IntWritable, DoubleWritable, Text, Text> {

	int k;
	List<Double> list;
	
	
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		for (DoubleWritable val : values) {
			list.add(val.get());
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException{
		list = new ArrayList<>();
		k = Integer.parseInt(context.getConfiguration().get("k"));
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException{
		Double[] finalList = (Double[]) list.toArray();
		Arrays.sort(finalList);
		for(int i = 0; i < k; k++){
			context.write(new Text("1"), new Text(finalList[k].toString()));
		}
		super.cleanup(context);
	}

}
