package log;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.junit.Test;

public class WebLog {
	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();

			Pattern p = Pattern.compile("\\[([^/]*/[^/]*/[^:]*:[^:]*).*\\]");
			Matcher m = p.matcher(line);
			Text data = new Text();
			data.set("01/Nov/2014:01");
			while (m.find()) {
				data.set(m.group(1));
			}

			context.write(data, new IntWritable(1));
		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private static IntWritable linenum = new IntWritable(0);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for (IntWritable val : values) {

				linenum = new IntWritable(linenum.get() + 1);

			}

			context.write(key, linenum);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WebLog");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: WebLog <in> <out>");
			System.exit(2);
		}

		job.setJarByClass(WebLog.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	@Test
	public void test() {
		Pattern p = Pattern.compile("^http");
		Matcher m = p.matcher("asa://http");
		if (m.find()) {
			System.out.println("true");
		} else {
			System.out.println("false");
		}

	}
}
