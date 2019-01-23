package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4_Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2){
			System.out.printf(
							"Usage: Q1_Driver <input dir> <outputdir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();

		job.setJarByClass(Q4_Driver.class);

		job.setJobName("Question Two Driver");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Q4_Mapper.class);
		job.setReducerClass(Q4_Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
