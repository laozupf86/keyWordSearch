package extendedClasses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Cluster;
//import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class ReadFileEInputFormat<K,V> extends SequenceFileInputFormat<K,V>{
	
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
		
		
		
		return null;
		
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

}
