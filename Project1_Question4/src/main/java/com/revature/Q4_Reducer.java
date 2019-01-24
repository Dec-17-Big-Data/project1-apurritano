package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author apurritano
 * 
 * This reducer method returns the key value pairs of the selected data points.
 *
 */

public class Q4_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException{
		
		for (DoubleWritable value : values){
			context.write(key, value);
		}
	}
}