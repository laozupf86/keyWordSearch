package mainPackage;

import mapperClasses.InvertedIndexMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import reducerClasses.InvertedIndexReducer;

public class BuildInvertedIndex {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		    Job job = Job.getInstance(conf, "build inverted index - haozhou");
		    job.setJarByClass(BuildInvertedIndex.class);
		    //job.setMapperClass(InvertedIndexMapper.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    //job.setCombinerClass(InvertedIndexReducer.class);
		    job.setCombinerClass(InvertedIndexReducer.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	

}
