package sort;
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
import org.apache.hadoop.util.GenericOptionsParser;



public class Sort {
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>
	{
		private static IntWritable data=new IntWritable();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, IntWritable, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line=value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}


	}

	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
	{

		private static IntWritable linenum = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key,Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(IntWritable val:values){

				context.write(linenum, key);

				linenum = new IntWritable(linenum.get()+1);

			} 
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Sort");
		String[] ioArgs=new String[]{"/sort/input","/sort/output"};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Sort <in> <out>");
			System.exit(2);
		}  

		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
