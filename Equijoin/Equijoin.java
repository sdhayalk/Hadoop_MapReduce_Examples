import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Equijoin	{
	public static int joinKeyIndex1;
	public static int joinKeyIndex2;

	// public Equijoin(int joinKeyIndex1, int joinKeyIndex2)	{
	// 	this.joinKeyIndex1 = joinKeyIndex1;
	// 	this.joinKeyIndex2 = joinKeyIndex2;
	// }

	public static class MapperClassTable1 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException	{
			String rowString = value.toString();
			String joinKeyString = rowString.split(",")[joinKeyIndex1];
			context.write(new Text(joinKeyString), new Text("1," + rowString));
		}
	}

	public static class MapperClassTable2 extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException	{
			String rowString = value.toString();
			String joinKeyString = rowString.split(",")[joinKeyIndex2];
			context.write(new Text(joinKeyString), new Text("2," + rowString));
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException	{
			String table1Row = "", table2Row = "";
			int numberOfTables = 0;

			for(Text val : values)	{
				String valString = values.toString();
				if(valString.substring(0,1).equals("1"))	{
					table1Row = valString.substring(2,4);

				}
				else if(valString.substring(0,1).equals("2")){
					table2Row = valString.substring(2,4);
				}
				numberOfTables++;
			}

			if(numberOfTables == 2)	{
				context.write(new Text(""), new Text(table1Row+","+table2Row));
			}
		}
	}

	public static void main(String args[]) throws Exception 	{
		Configuration conf = new Configuration();       // define a configuration
	    Job job = Job.getInstance(conf, "equijoin");	// define a job
	    job.setJarByClass(Equijoin.class);
		
		MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, MapperClassTable1.class);
 		joinKeyIndex1 = Integer.parseInt(args[1]);
 		MultipleInputs.addInputPath(job,new Path(args[2]), TextInputFormat.class, MapperClassTable2.class);
 		joinKeyIndex2 = Integer.parseInt(args[3]);
 		FileOutputFormat.setOutputPath(job, new Path(args[4]));

 		job.setReducerClass(ReducerClass.class);
 		job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);              // type (i.e. class) of output key
	    job.setOutputValueClass(Text.class);     		// type (i.e. class) of output value
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}