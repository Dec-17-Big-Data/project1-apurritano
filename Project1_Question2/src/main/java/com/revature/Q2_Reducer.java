package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author apurritano
 *
 * This reducer method takes the average increase between each year and adds it to a variable.
 * It then divides that sum by the count of values it took in.
 * This returns the average increase for each category.
 */

public class Q2_Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException{
		double sum = 0;
		double counter = 0;
		
		for (DoubleWritable value : values){
			sum += value.get();
			counter++;
		}
		
		double average = sum / counter;
		context.write(key, new DoubleWritable(average));
	}
}
