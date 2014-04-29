package com.virtualeleven.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMain extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// fs.default.name seems to be deprecated
		conf.set("fs.defaultFS", "192.168.120.100:9000");
		conf.set("mapred.job.tracker", "192.168.120.100:9001");

		conf.set("fs.default.name", "file:///");

		Job job = Job.getInstance(conf, "Word Count");
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapperClass(WordCountMapper.class);
		job.setJarByClass(WordCountMain.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WordCountMain(), args);
		System.exit(exitCode);
	}
}
