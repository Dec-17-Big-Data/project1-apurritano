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
 * List the average increase in female education in the U.S. from the year 2000.
 * 
 * The thought process here was a slightly more complicated than the first question.
 * I looked through the data that was available for females in the US;
 * I believed that looking at 'Educational attainment, at least completed..' with
 *  each of the education levels was the best available data to use for this question.
 * I thought it was a best statistic than 'School Enrollment' because simply being
 * enrolled in school does not necessarily mean that they passed and graduated and became more educated.
 * Once you graduate it proves that you became more educated.
 *
 * 
 * I split data by the commas and created an if statement to only get the USA data.
 * I then located six different categories: 
 * 'Educational attainment, at least completed [primary, lower secondary, upper secondary, 
 *  short-term tertiary, bachelors, & masters] population 25+ years, female (%) (cumulative)'
 * After that I took the earliest value starting at year 2000 moving forward and the latest value.
 * Then set the increasedEdu variable to the difference between the two values.
 * I then set the education type to each category based on the third index.
 * Finally made a write statement to print the education type and the percent it increased from year 2000
 *
 */

public class Q2_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString(); 
		
		String[] data =  line.split("\",\"?");
	
		if (data[1].equals("USA")){
			if (data[3].equals("SE.PRM.CUAT.FE.ZS")
					|| data[3].equals("SE.SEC.CUAT.LO.FE.ZS")
					|| data[3].equals("SE.SEC.CUAT.UP.FE.ZS")
					|| data[3].equals("SE.TER.CUAT.ST.FE.ZS")
					|| data[3].equals("SE.TER.CUAT.BA.FE.ZS")
					|| data[3].equals("SE.TER.CUAT.MS.FE.ZS")
					){
				double earliestEdu = 0;
				double latestEdu = 0;
				double increasedEdu = 0;
				
				for (int i = 44; i < data.length - 1; i++){
					if (data[i].equals("")) continue;
					double stat = 0;
					stat = Double.parseDouble(data[i]);
					if (earliestEdu == 0){
						earliestEdu = stat;
						latestEdu = stat;
					}
					earliestEdu = latestEdu;
					latestEdu = stat;
					increasedEdu = (latestEdu - earliestEdu);
					
					String typeEdu = null;
					if (data[3].equals("SE.PRM.CUAT.FE.ZS")){
						typeEdu = "Primary Education: ";
					}else if(data[3].equals("SE.SEC.CUAT.LO.FE.ZS")){
						typeEdu = "Lower Secondary Education: ";
					}else if(data[3].equals("SE.SEC.CUAT.UP.FE.ZS")){
						typeEdu = "Upper Secondary Education: ";
					}else if(data[3].equals("SE.TER.CUAT.ST.FE.ZS")){
						typeEdu = "Short-term Tertiary Education: ";
					}else if(data[3].equals("SE.TER.CUAT.BA.FE.ZS")){
						typeEdu = "Bachelors Degree: ";
					}else if(data[3].equals("SE.TER.CUAT.MS.FE.ZS")){
						typeEdu = "Masters Degree: ";
					}
					context.write(new Text("The average increase from year 2000 in females that at least completed their " + typeEdu), new DoubleWritable(increasedEdu));
				}
			}
		}
	}
}
