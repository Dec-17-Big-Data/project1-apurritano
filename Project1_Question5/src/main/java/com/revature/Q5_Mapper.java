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
 * For the custom question, I chose to look at the Unemployment with advanced education in females because I believed
 * this was valuable data that was available in the data set. I also wanted to compare this percent to the 
 * Time-related Underemployment percent in females. 
 * "Time-related underemployment refers to all persons in employment who 
 * (i) wanted to work additional hours, (ii) had worked less than a specified hours threshold (working time in all jobs), 
 * and (iii) were available to work additional hours given an opportunity for more work." 
 * Source: (https://datacatalog.worldbank.org/time-related-underemployment-female-employment)
 * I thought this was interesting to look at and compare to see how many people are working and how this effects the amount of work.
 *
 * I split the data file by each comma once again. Then filtered results to only get the USA data.
 * Then filtered that data by the two categories I mentioned above.
 * Then I created an iteration to grab all the data points for the year 2000 to the latest data.
 * I then set a variable for each category to the name of that type.
 * Finally created a write statement to print each data point for each category.
 * 
 */

public class Q5_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		String[] data =  line.split("\",\"?");
		
		if (data[1].equals("USA")){
			if (data[3].equals("SL.UEM.ADVN.FE.ZS")
					|| data[3].equals("SL.EMP.UNDR.FE.ZS")){
				int year = 0;
				double empValue = 0;
				String type = null;
				
				for (int i = 44; i < data.length; i++){
					if (data[i].equals("")) continue;
					empValue = Double.parseDouble(data[i]);
					year = i + 1956;
					
					if (data[3].equals("SL.UEM.ADVN.FE.ZS")){
						type = "USA female unemployment rate with advanced education in ";
					}else if(data[3].equals("SL.EMP.UNDR.FE.ZS")){
						type = "USA female time-related underemployment in ";
				
					}
					context.write(new Text(type + year + ": "), new DoubleWritable(empValue));
				}
			}
		}
	}
}
