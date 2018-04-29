package part1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;  
import org.apache.hadoop.mapred.JobConf;  
import org.apache.hadoop.mapred.JobID;  
import org.apache.hadoop.mapred.Reporter;  
import org.apache.hadoop.mapred.RunningJob;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;

public class WordCountReducer extends MapReduceBase
	implements Reducer<Text, Value, Text, String> {

	private Long SUM;	
	RunningJob parentJob;
	@Override  
        public void configure(JobConf conf) {  
            try {  
                JobClient client = new JobClient(conf);  
                parentJob = client.getJob(JobID.forName(conf.get("mapred.job.id") ));  
        
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
            super.configure(conf);  
        }

	public void reduce(Text key, Iterator<Value> values,
		OutputCollector<Text, String> output, Reporter reporter) throws IOException {	
		
		Value reduceValue = new Value();
		
		while(values.hasNext()) {
			Value v = values.next();
			reduceValue.merge(v);
		}	
		
		reduceValue.sort();
		
		// TODO : You have to modify something for output result
		output.collect(new Text(key.toString()), reduceValue.outputValue());
	}
}
