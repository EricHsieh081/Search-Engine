package part1;

import java.io.IOException;

import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCountMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Value> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Value> output, 
		Reporter reporter) throws IOException{
	
			Hashtable<String, Value> table = new Hashtable<String, Value>();
			String filename;
			
			String line = value.toString();
			Matcher pageMatcher = Pattern.compile("<page>(.*?)</page>").matcher(line);

			Value val = new Value();
			int offset;
			String page, text, word, title = "ini";
			String linksPage;

			while(pageMatcher.find()) {
				page = pageMatcher.group();

				filename = ((FileSplit)reporter.getInputSplit()).getPath().getName();

				Matcher titleMatcher = Pattern.compile("<title>(.*?)</title>").matcher(page);	
				while(titleMatcher.find()) {
					title = titleMatcher.group(1);
				}

				Matcher textMatcher = Pattern.compile("<text(.*?)</text>").matcher(page);
				while(textMatcher.find()) {
					text = textMatcher.group(1);
					Matcher wordMatcher = Pattern.compile("[A-Za-z]+").matcher(text);
						while(wordMatcher.find()) {
							word = wordMatcher.group();
							val = table.get(word);
							if (val == null) {
								val = new Value(title);
								table.put(word, val);
							}
							offset = (int)key.get() + wordMatcher.start();
							//val.addOffset(title, offset);
						}
				}


				reporter.incrCounter("wordCounter", filename, 1);
			}

			

			Enumeration e = table.keys();
			while(e.hasMoreElements()){
				String keyW = e.nextElement().toString();
				Value v = table.get(keyW);
				output.collect(new Text(keyW), v);
			}
			/*
			String delimiter = "[a-zA-Z]+"; 
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, delimiter);
			
			while (tokenizer.hasMoreTokens()) {
				String aWord = tokenizer.nextToken();
				output.collect(new Text(aWord), new IntWritable(1));
			}*/
	}
}
