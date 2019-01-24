package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author apurritano
 * 
 * Here I simply reduced the results to only display the graduation ratio's that were less than 30% 
 *
 */

public class Q1_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double results = 30;	

		for (DoubleWritable value : values){
			if (value.get() < results){
				context.write(key, value);
			}
		
		}
	}
}
