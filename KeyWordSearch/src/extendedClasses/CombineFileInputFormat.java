package extendedClasses;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CombineFileInputFormat extends FileInputFormat<IntWritable,DoubleWritable>{

	/**
	 * int - point id
	 * double - distance from query distance
	 */
	
	@Override
	public RecordReader<IntWritable, DoubleWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {

		return false;
	}

	

}
