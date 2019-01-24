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
 * List the % of change in male employment from the year 2000. 
 *
 * For this question, my thought process was to get the percent of employment in 2000
 * as well as the latest available data and display the difference.
 * I split on each comma and filtered results to 'Employment to population ratio, 15+, male (%) (modeled ILO estimate),
 * I chose to use the modeled ILO estimate over the national estimate because I assumed the ILO was a better 
 * representation of the true value than the national estimate. 
 * I then create two loops that would get the latest data point and the earliest starting from year 2000.
 * After each I assigned these values to a variable and subtracted the earliest from the latest to get the 
 * change in percent. 
 * Finally I created a write statement to print the country name with the change in percent.
 * I filtered these results to emit the values with a 0% change assuming this was null data points.
 *
 */

public class Q3_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString(); 
		
		String[] data =  line.split("\",\"?");
		
		if (data[3].equals("SL.EMP.TOTL.SP.MA.ZS")){
			double earliestEmp = 0;
			double latestEmp = 0;
			double changeEmp = 0;
			
			for (int i = 44; i < data.length; i++){
				if (data[i].equals("")) continue;
				latestEmp = Double.parseDouble(data[i]);		
			}
			
			for (int i = data.length - 1; i > 43; i--){
				if (data[i].equals("")) continue;
				earliestEmp = Double.parseDouble(data[i]);
			}
			
			changeEmp = (latestEmp - earliestEmp);
			
			if (changeEmp != 0){
				context.write(new Text("The percent of change in male employment from 2000 in " + data[0].replaceAll("^\"|\"$","") + " is: "), new DoubleWritable(changeEmp));
				
				//Output used to create graph
				//context.write(new Text(data[0].replaceAll("^\"|\"$","")), new DoubleWritable(changeEmp));
			}	
		}
	}
}
