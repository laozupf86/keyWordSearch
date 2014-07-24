package mainPackage;

import mapperClasses.InvertedIndexMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import extendedClasses.ReadFileEInputFormat;
import extendedClasses.TestP;
import reducerClasses.InvertedIndexReducer;

public class BuildInvertedIndex extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildInvertedIndex(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJobName("inveted index");
		// System.out.println("start");
		job.setJarByClass(BuildInvertedIndex.class);
		job.setInputFormatClass(ReadFileEInputFormat.class);

		job.setMapperClass(InvertedIndexMapper.class);

		//job.setPartitionerClass(TestP.class);

		// job.setCombinerClass(InvertedIndexReducer.class);
		job.setReducerClass(InvertedIndexReducer.class);
		// System.out.println("here");
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// MultipleOutputs.addNamedOutput(job, "a", TextOutputFormat.class,
		// Text.class, Text.class);

		//FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	

}
