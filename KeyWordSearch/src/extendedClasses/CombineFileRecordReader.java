package extendedClasses;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import dataStructureClasses.Point;

public class CombineFileRecordReader extends RecordReader<IntWritable,DoubleWritable>{

	private FileSplit fileSplit;
	private FSDataInputStream in = null;
	
	private IntWritable key = null;
	private DoubleWritable value = null;
	
	private boolean processed = false;
	
	private BufferedReader bufferedReader;
	
	@Override
	public void close() throws IOException {
		bufferedReader.close();
		IOUtils.closeStream(in);
		
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException,
			InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		fileSplit = (FileSplit) split;
		System.out.println("InputSplit is " + fileSplit.getPath().getName());
		Configuration job = context.getConfiguration();
		Path file = fileSplit.getPath();
		FileSystem temp = file.getFileSystem(job);
		in = temp.open(file);
		bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
	}

	
	/**
	 * the key is the point id, and the value is the distance between query and this point
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null){
			key = new IntWritable();
			//key.set(1L);
		}
		
		if(value == null){
			value = new DoubleWritable();
		}
		String line = null;
		
		if((line = bufferedReader.readLine()) != null){
			String[] words = line.split(",");
			key.set(Integer.parseInt(words[0]));
			value.set(Double.parseDouble(words[1]));
			System.out.println("key is " + key.get() + " ,value is " + value.get());
			return true;
		}else{
			return false;
		}
	
	}

}
