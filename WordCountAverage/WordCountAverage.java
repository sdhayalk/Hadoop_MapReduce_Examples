/*
  The following code has been exactly used from Apache Hadoop tutorial
  referenced and used from: https://hadoop.apache.org/docs/r2.7.2/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
*/
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

public class WordCountAverage {

  public static class MapperClass extends Mapper<Object, Text, Text, IntWritable>{  // mapper of type <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    private Text word = new Text(); // declare the key
    private final static IntWritable one = new IntWritable(1);  // declare the value

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());  // convert input-value into tokens, which are iterable
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());  // set (i.e. assign) to key, where key is word. 
        context.write(word, one);   // write key and value, where key is word and value is 1. context.write(Text, IntWritable)
      }
    }
  }

  public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(); // decalare the value

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      float count = 0;
      for (IntWritable val : values) {
        sum += (float)val.get(); // get the actual int value
        count += 1;
      }


      result.set((float)sum/count);    // set the value as number of occurences of the key
      context.write(key, result); // write key and value, where key is the word, and value is number of occurences of the key
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();       // define a configuration
    Job job = Job.getInstance(conf, "word count average");  // define a job
    job.setJarByClass(WordCountAverage.class);
    job.setMapperClass(MapperClass.class);          // set the mapper to the job
    job.setCombinerClass(ReducerClass.class);       // does local aggregation
    job.setReducerClass(ReducerClass.class);        // set the reducer to the job
    job.setOutputKeyClass(Text.class);              // type (i.e. class) of output key
    job.setOutputValueClass(IntWritable.class);     // type (i.e. class) of output value

    FileInputFormat.addInputPath(job, new Path(args[0]));   // input path specification
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path specification
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}