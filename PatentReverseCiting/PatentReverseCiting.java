import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PatentReverseCiting	{
	public class MapperClass extends Mapper<Text, Text, Text, Text>	{
		private Text citedKey;
		private Text citingValue;

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException	{
			String keyString = key.toString();
			String[] keyStringArray = keyString.split(",");

			citedKey = new Text(keyStringArray[1]);
			citingValue = new Text(keyStringArray[0]);
			context.write(citedKey, citingValue);
		}
	}

	public class ReducerClass extends Reducer<Text, Text, Text, Text>	{
		private Text reverseCitingsValue;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException	{
			String reverseCitingsString = "";
			for(Text val : values)	{
				String valString = val.toString();
				reverseCitingsString = reverseCitingsString + valString + ", ";
			}

			reverseCitingsValue = new Text(reverseCitingsString);
			context.write(key, reverseCitingsValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();       // define a configuration
	    Job job = Job.getInstance(conf, "PatentReverseCiting");  // define a job
	    job.setJarByClass(PatentReverseCiting.class);
	    job.setMapperClass(MapperClass.class);          // set the mapper to the job
	    job.setCombinerClass(ReducerClass.class);       // does local aggregation
	    job.setReducerClass(ReducerClass.class);        // set the reducer to the job
	    job.setOutputKeyClass(Text.class);              // type (i.e. class) of output key
	    job.setOutputValueClass(Text.class);     		// type (i.e. class) of output value
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));   // input path specification
	    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path specification
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}