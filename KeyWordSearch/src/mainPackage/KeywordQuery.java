package mainPackage;

import mapperClasses.CombineMapper;
import mapperClasses.SearcherMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import reducerClasses.CombineReducer;
import reducerClasses.InvertedIndexReducer;
import extendedClasses.CombineFileInputFormat;
import extendedClasses.ReadIndexedFileInputFormat;
import extendedClasses.SearchResultOutputFormat;

public class KeywordQuery extends Configured implements Tool {

	public static void main(String[] args) {
		if(args.length != 7){
			System.err.println("invaild parameter number");
			System.exit(2);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("point", args[3]);
		conf.set("keyword", args[4]);
		conf.set("k", args[5]);
		int numberOfReducer = Integer.parseInt(args[6]);
		
		Job job = Job.getInstance(conf);
		job.setJobName("Keyword Query");
		System.out.println("search started");
		//start search
		job.setJarByClass(KeywordQuery.class);
		job.setInputFormatClass(ReadIndexedFileInputFormat.class);

		job.setMapperClass(SearcherMapper.class);


		// job.setCombinerClass(InvertedIndexReducer.class);
		job.setReducerClass(InvertedIndexReducer.class);
		// System.out.println("here");
		job.setNumReduceTasks(numberOfReducer);
		
		job.setOutputFormatClass(SearchResultOutputFormat.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
		// MultipleOutputs.addNamedOutput(job, "a", TextOutputFormat.class,
		// Text.class, Text.class);

		//FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean firstWork = job.waitForCompletion(true);
		
		if(!firstWork){
			return 1;
		}
		
		//start combine
		
		Job combineJob = Job.getInstance(conf);
		
		combineJob.setJobName("Keyword Quyer step 3");
		combineJob.setJarByClass(KeywordQuery.class);
		job.setInputFormatClass(CombineFileInputFormat.class);
		job.setMapperClass(CombineMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(CombineReducer.class);
		FileInputFormat.setInputPaths(combineJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(combineJob, new Path(args[2]));
		
		
		
		return combineJob.waitForCompletion(true) ? 0 : 1;
		
		
	}

}
