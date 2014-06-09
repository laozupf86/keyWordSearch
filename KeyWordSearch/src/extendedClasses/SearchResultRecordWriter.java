package extendedClasses;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dataStructureClasses.Point;

public class SearchResultRecordWriter extends RecordWriter<Point, DoubleWritable>{

	public DataOutputStream out;
	public String lineS;
	
	public SearchResultRecordWriter(DataOutputStream out){
		this.out = out;
		this.lineS = System.getProperty("line.separator");
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		out.close();
		
	}

	@Override
	public void write(Point key, DoubleWritable value) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		out.writeBytes(key.toString() + "," + value.get() + lineS);
		
	}

}
