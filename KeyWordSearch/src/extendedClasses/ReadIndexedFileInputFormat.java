package extendedClasses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReadIndexedFileInputFormat extends FileInputFormat<Text,ArrayWritable>{

	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {

		return false;
	}

	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException{
		TaskTrackerInfo[] servers = null;
		try {
			servers = this.getActiveServerList(job);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		List<InputSplit> split = new ArrayList<>();
		List<FileStatus> files = this.listStatus(job);
		Map<String, Integer> words = new HashMap<>();
		String[] word = job.getConfiguration().get("keywords").split(",");
		for (String w : word){
			words.put(w, 0);
		}
		
		
		
		
		int currentServer = 0;
		for(FileStatus file: files){
			if(words.containsKey(file.getPath().getName())){
				//assign each single file into a single server
				split.add(new FileSplit(file.getPath(), 0, file.getLen(), 
						new String[]{servers[0].getTaskTrackerName()}));
				currentServer = getNextServer(currentServer, servers.length);
				System.out.println("file name is " + file.getPath().getName());
			}else{
				System.out.println("not match, file name is " + file.getPath().getName());
			}
			
		}
		
		return split;
		
	}
	
	private TaskTrackerInfo[] getActiveServerList(JobContext job) throws IOException, InterruptedException{
		Cluster cluster = new Cluster(job.getConfiguration());
		TaskTrackerInfo[] status = cluster.getActiveTaskTrackers();
	//	for(TaskTrackerInfo serverInfo : status){
	//		serverInfo.getTaskTrackerName();
	//	}
			
		return status;
		
	}
	
	private static int getNextServer(int current, int max){
		current++;
		if(current >= max){
			current = 0;
		}
		return current;
	}
	
	
	@Override
	public RecordReader<Text, ArrayWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new IndexedFileRecordReader();
	}

}
