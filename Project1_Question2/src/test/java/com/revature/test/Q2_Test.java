package com.revature.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.Q2_Mapper;
import com.revature.Q2_Reducer;

public class Q2_Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		Q2_Mapper mapper = new Q2_Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q2_Reducer reducer = new Q2_Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException{
		
		String str = "\"United States\",\"USA\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\","
				+ "\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.7\",\"31.8\",\"31.9\",\"\",";

				
				mapDriver.withInput(new LongWritable(1), new Text(str));
				
				mapDriver.withOutput(new Text("The average increase from year 2000 in females that at least completed their Bachelors Degree: "), new DoubleWritable(.1));
				
				mapDriver.run();
		
	}
	
	@Test
	public void testReducer(){
		
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(.5));
		
		reduceDriver.withInput(new Text("Masters Degree"), values);
		
		reduceDriver.withOutput(new Text("Masters Degree"), new DoubleWritable(.5));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException{
		
		String str = "\"United States\",\"USA\",\"Educational attainment, at least Master's or equivalent, population 25+, female (%) (cumulative)\","
				+ "\"SE.TER.CUAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.2\",\"11.4\",\"11.6\",\"\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("The average increase from year 2000 in females that at least completed their Masters Degree: "), new DoubleWritable(.2));
		
		mapDriver.run();
	}

}
