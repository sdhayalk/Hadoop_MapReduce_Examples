import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramCounter {
	public static class AnagramMapper extends Mapper<Object, Text, Text, IntWritable>	{
		private Text sortedWordKey = new Text();	// declare the key
		private final static IntWritable oneValue = new IntWritable(1);		// define the value

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException	{
			StringTokenizer token = new StringTokenizer(value.toString());

			while(token.hasMoreTokens())	{
				String tempWord = token.nextToken();
				char[] tempWordCharArray = tempWord.toCharArray();
				Arrays.sort(tempWordCharArray);
				String sortedTempWord = new String(tempWordCharArray);
				sortedWordKey.set(sortedTempWord);		// set the key to the sorted word

				context.write(sortedWordKey, oneValue);	// write key value pair to context
			}
		}
	}

	public static class AnagramReducer extends Reducer<Text, IntWritable, Text, IntWritable>	{
		private Text sortedWordKey = new Text();	// declare the key
		private IntWritable countAnagramsValue = new IntWritable();	// declare the value

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException	{
			int count = 0;
			for(IntWritable val : values)
				count = count + val.get();	// get the actual int value

			if(count >= 2)	{	// if count >= 2 which means there is an anagram present
				sortedWordKey.set(key.toString());	// set the key to be the sorted anagram word
				countAnagramsValue.set(count);		// set the value to be count of that sorted anagram word
				context.write(sortedWordKey, countAnagramsValue);	// write the key value pair to context
			}
		}
	}

	public static void main(String args[]) throws Exception	{
		Configuration conf = new Configuration();       	// define a configuration
	    Job job = Job.getInstance(conf, "anagram counter");	// define a job
	    job.setJarByClass(AnagramCounter.class);
	    job.setMapperClass(AnagramMapper.class);      		// set the mapper to the job
	    job.setCombinerClass(AnagramReducer.class);      	// does local aggregation
	    job.setReducerClass(AnagramReducer.class);       	// set the reducer to the job
	    job.setOutputKeyClass(Text.class);              	// type (i.e. class) of output key
	    job.setOutputValueClass(IntWritable.class);     	// type (i.e. class) of output value
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));   // input path specification
	    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path specification
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}