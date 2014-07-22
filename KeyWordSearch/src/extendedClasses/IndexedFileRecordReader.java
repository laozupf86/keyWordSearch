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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import dataStructureClasses.Point;

public class IndexedFileRecordReader extends RecordReader<Text,ArrayWritable>{

	
	private FileSplit fileSplit;
	private FSDataInputStream in = null;
	
	private Text key = null;
	private ArrayWritable value = null;
	
	private boolean processed = false;
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
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
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		fileSplit = (FileSplit) split;
		//System.out.println("host name in file is " + fileSplit.)
		//String hostname = java.net.InetAddress.getLocalHost().getHostName();
		//System.out.println("fileSplit is " + fileSplit.getLocations().length);
		System.out.println("InputSplit is " + fileSplit.getPath().getName());
		Configuration job = context.getConfiguration();
		Path file = fileSplit.getPath();
		FileSystem temp = file.getFileSystem(job);
		in = temp.open(file);
		
	}

	/**
	 * get the key-value pairs from hadoop generated file directly
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null){
			key = new Text();
			//key.set(1L);
		}
		
		if(value == null){
			value = new ArrayWritable(Point.class);
		}
		
		if(!processed){
			//long currentKey = key.get();
			//key.set(fileSplit.getPath().getName());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			List<Point> fileStream = new ArrayList<>();
			String line = null;
			line = bufferedReader.readLine();
			//split by tab
			String[] firstWord = line.split("//t");
			key.set(firstWord[0]);
			//get the key of each line
			String[] pointLine = firstWord[1].split(",");
			Point point = new Point(Integer.parseInt(pointLine[0]), Float.parseFloat(pointLine[1]),
					Float.parseFloat(pointLine[2]));
			fileStream.add(point);
			//read the rest points from file
			while((line = bufferedReader.readLine()) != null){
				System.out.println("read file is " + line);
				pointLine = line.split(",");
				point = new Point(Integer.parseInt(pointLine[0]), Float.parseFloat(pointLine[1]),
						Float.parseFloat(pointLine[2]));
				fileStream.add(point);
			}
			System.out.println("start to cast");
			Point[] texts = new Point[fileStream.size()];
			texts = fileStream.toArray(texts);
			value.set(texts);
			System.out.println("cast finish");
			bufferedReader.close();
			IOUtils.closeStream(in);
			processed = true;
			return true;
		}
		
		return false;
	}
	

}
