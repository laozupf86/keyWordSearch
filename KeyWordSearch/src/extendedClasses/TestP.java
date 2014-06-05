package extendedClasses;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TestP extends Partitioner<Text,Text> implements Configurable{

	int i = 0;
	private Configuration conf = null;
	private static final Log log = LogFactory.getLog(TestP.class); 
	
	@Override
	public int getPartition(Text key, Text value, int numP) {
		
		i++;
		System.out.println("key is " + key.toString() + " value is " + value.toString() + " count is " + i);
		log.warn("key is " + key.toString() + " value is " + value.toString() + " count is " + i);
		
		return i%numP;
	}


	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}


	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		i = 1;
		this.conf = conf;
		log.warn("set conf");
	}

}
