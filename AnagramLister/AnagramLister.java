import java.io.IOException;
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


		private Text sortedWordKey = new Text();	// declare the key
		private final static IntWritable oneValue = new IntWritable(1);		// define the value

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException	{
			StringTokenizer token = new StringTokenizer(value.toString());

			while(token.hasMoreTokens())	{
				String tempWord = value.toString();
				char[] tempWordCharArray = tempWord.toCharArray();
				Arrays.sort(tempWordCharArray);
				String sortedTempWord = new String(tempWordCharArray);
				System.out.println(sortedTempWord);
				sortedWordKey.set(sortedTempWord);		// set the key to the sorted word

				context.write(sortedWordKey, oneValue);	// write key value pair to context
			}
		}
	}
}