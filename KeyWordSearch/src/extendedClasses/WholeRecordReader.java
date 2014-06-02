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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.IOUtils;


public class WholeRecordReader extends RecordReader<LongWritable,ArrayWritable>{

	private FileSplit fileSplit;
	private FSDataInputStream in = null;
	
	private LongWritable key = null;
	private ArrayWritable value = null;
	
	private boolean processed = false;
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public ArrayWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fileSplit = (FileSplit) arg0;
		Configuration job = arg1.getConfiguration();
		Path file = fileSplit.getPath();
		FileSystem temp = file.getFileSystem(job);
		in = temp.open(file);
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(key == null){
			key = new LongWritable();
			key.set(1L);
		}
		
		if(value == null){
			value = new ArrayWritable(org.apache.hadoop.io.Text.class);
		}
		
		if(!processed){
			long currentKey = key.get();
			key.set(currentKey + 1);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			List<Text> fileStream = new ArrayList<>();
			String line = null;
			while((line = bufferedReader.readLine()) != null){
				Text text = new Text();
				text.set(line);
				fileStream.add(text);
			}
			value.set((Text[]) fileStream.toArray());
			bufferedReader.close();
			IOUtils.closeStream(in);
			processed = true;
			return true;
		}
		
		return false;
	}

}
