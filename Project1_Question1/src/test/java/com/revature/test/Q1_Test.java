
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

import com.revature.Q1_Mapper;
import com.revature.Q1_Reducer;

public class Q1_Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		Q1_Mapper mapper = new Q1_Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q1_Reducer reducer = new Q1_Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		
		String str = "\"United States\",\"USA\",\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\","
		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
		+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"26.43332\","
		+ "\"26.65924\",\"26.42296\",\"26.89631\",\"27.52726\",\"27.99822\",\"28.22203\",\"28.85868\",\"29.63534\","
		+ "\"30.33826\",\"31.01373\",\"\",\"32.7659\",\"25.5\",\"\",\"\",\"\",\"\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("United State's female graduation rate in 2012: "), new DoubleWritable(25.5));
		
		mapDriver.run();
			
	}
	
	@Test
	public void testReducer(){
		
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(15));
		
		reduceDriver.withInput(new Text("Argentina"), values);
		
		reduceDriver.withOutput(new Text("Argentina"), new DoubleWritable(15));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		
		String str = "\"Argentina\",\"ARG\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"10.29958\",\"8.60549\",\"13.46936\",\"14.3721\",\"\",\"15.76742\",\"13.72042\",\"14.12948\",'"
				+ "\"14.65367\",\"\",\"\",\"14.09986\",\"13.82683\",\"13.7\",\"\",\"\",";
		
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(str));
		
		mapReduceDriver.addOutput(new Text("Argentina's female graduation rate in 2014: "), new DoubleWritable(13.7));
		
		mapReduceDriver.runTest();
		
	}

}

