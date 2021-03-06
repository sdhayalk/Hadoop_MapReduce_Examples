import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramLister	{

	public static class AnagramListerMapper extends Mapper<Object, Text, Text, Text>	{
		private Text sortedWordKey = new Text();	// declare the key
		private Text actualWordValue = new Text();	// declare the value

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException	{
			StringTokenizer token = new StringTokenizer(value.toString());

			while(token.hasMoreTokens())	{
				String tempWord = token.nextToken();
				char[] tempWordCharArray = tempWord.toCharArray();
				Arrays.sort(tempWordCharArray);
				String sortedTempWord = new String(tempWordCharArray);

				sortedWordKey.set(sortedTempWord);
				actualWordValue.set(tempWord);

				context.write(sortedWordKey, actualWordValue);
			}
		}
	}


	public static class AnagramListerReducer extends Reducer<Text, Text, Text, Text>	{
		private Text correspondingAnagramsValue;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException	{
			HashSet<String> hashset = new HashSet<String>();
			String resultString = "";

			for(Text val : values)	{
				String temp = val.toString();	// not val.get(). IntWritables have val.get()
				hashset.add(temp);
			}

			Iterator<String> iterator = hashset.iterator();
			while(iterator.hasNext())	{
				String temp = iterator.next();
				resultString = resultString + temp + ",";
			}

			StringTokenizer token = new StringTokenizer(resultString,",");
			if(token.countTokens() >= 2)	{
				correspondingAnagramsValue = new Text(resultString);
				context.write(key, correspondingAnagramsValue);
			}

		}
	}


	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();       // define a configuration
	    Job job = Job.getInstance(conf, "anagram lister");  // define a job
	    job.setJarByClass(AnagramLister.class);
	    job.setMapperClass(AnagramListerMapper.class);          // set the mapper to the job
	    job.setCombinerClass(AnagramListerReducer.class);       // does local aggregation
	    job.setReducerClass(AnagramListerReducer.class);        // set the reducer to the job
	    job.setOutputKeyClass(Text.class);              // type (i.e. class) of output key
	    job.setOutputValueClass(Text.class);     		// type (i.e. class) of output value
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));   // input path specification
	    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path specification
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}