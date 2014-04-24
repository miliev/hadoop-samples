package com.virtualeleven.datajoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataJoinJob extends Configured implements Tool {

	/**
	 * Join Orders.txt and Customer.txt data sources
	 * 
	 */
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, DataJoinJob.class);
		job.setJarByClass(getClass());

		FileInputFormat
				.setInputPaths(job, new Path(args[0]), new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setJobName("DataJoin");
		job.setMapperClass(DataJoinMap.class);
		job.setReducerClass(DataJoinReduce.class);

		job.setInputFormat(TextInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TaggedWritable.class);
		job.set("mapreduce.output.textoutputformat.separator", ",");

		// submit a job and pull for progress
		JobClient.runJob(job);
	    return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DataJoinJob(), args);
		System.exit(res);
    }
}