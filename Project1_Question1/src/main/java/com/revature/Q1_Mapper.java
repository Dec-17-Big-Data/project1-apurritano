package com.revature;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author apurritano
 * 
 * Identify the countries where % of female graduates is less than 30%.
 *
 * Thought process for this question was to get the latest data point on 'Gross graduation ratio, tertiary, female (%)' for each country. 
 * I split the csv file by each comma then created and if statement to locate the specific category I wanted.
 * Then created an iteration that starts on the last data cell and moves backwards till it receives a non-null value.
 * I then set my variable 'femaleGrads' to the latest data value and set my variable 'year' to the index of the value + 1956.
 * I got 1956 because the first year stated is 1960 and this is the 4th index (1960 - 4 = 1956).
 * Finally I created a write statement to print the county's name replacing unwanted characters, the year of the latest value, and the value.
 * And the write statement only writes the non 0 values assuming that a 0% graduation ratio is simply null data.
 * 
 */

public class Q1_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString(); 
		
		String[] data =  line.split("\",\"?");
	
		if (data[3].equals("SE.TER.CMPL.FE.ZS")){
			double femaleGrads = 0;
			int year = 0;
			for (int i = data.length - 1; i > 3; i--){
				if (data[i].length() > 0){
					data[i] = data[i];
					femaleGrads = Double.parseDouble(data[i]);
					year = i + 1956;
					break;
			}
		}	
		if (femaleGrads != 0.0){
			context.write(new Text(data[0].replaceAll("^\"|\"$","") + "'s female graduation rate in " + year + ": "), new DoubleWritable(femaleGrads));
			
			//Output used to create graph
			//context.write(new Text(data[0].replaceAll("^\"|\"$","")), new DoubleWritable(femaleGrads));
			}
		}
	}
}

