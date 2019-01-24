package com.revature.test;

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

import com.revature.Q5_Mapper;
import com.revature.Q5_Reducer;

public class Q5_Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		Q5_Mapper mapper = new Q5_Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q5_Reducer reducer = new Q5_Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		String str = "\"United States\",\"USA\",\"Time-related underemployment, female (% of employment)\",\"SL.EMP.UNDR.FE.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"1\",\"2\",\"3\","
				+ "\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2000: "), new DoubleWritable(0));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2001: "), new DoubleWritable(1));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2002: "), new DoubleWritable(2));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2003: "), new DoubleWritable(3));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2004: "), new DoubleWritable(4));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2005: "), new DoubleWritable(5));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2006: "), new DoubleWritable(6));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2007: "), new DoubleWritable(7));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2008: "), new DoubleWritable(8));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2009: "), new DoubleWritable(9));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2010: "), new DoubleWritable(10));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2011: "), new DoubleWritable(11));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2012: "), new DoubleWritable(12));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2013: "), new DoubleWritable(13));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2014: "), new DoubleWritable(14));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2015: "), new DoubleWritable(15));
		mapDriver.withOutput(new Text("USA female time-related underemployment in 2016: "), new DoubleWritable(16));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(17));
		
		reduceDriver.withInput(new Text("USA"), values);
		
		reduceDriver.withOutput(new Text("USA"), new DoubleWritable(17));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		String str = "\"United States\",\"USA\",\"Unemployment with advanced education, female (% of female labor force with advanced education)\","
				+ "\"SL.UEM.ADVN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"2.59999990463257\",\"2.95000004768372\",\"2.6800000667572\",\"2.35999989509583\",\"2.05999994277954\","
				+ "\"2.55999994277954\",\"3.01999998092651\",\"1.60000002384186\",\"1.57000005245209\",\"3.75\",\"3.48000001907349\","
				+ "\"3.10999989509583\",\"2.72000002861023\",\"2.46000003814697\",\"2.27999997138977\",\"0\",\"1\","
				+ "\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\",\"11\",\"12\",\"13\",\"14\",\"15\",\"16\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2000: "), new DoubleWritable(0));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2001: "), new DoubleWritable(1));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2002: "), new DoubleWritable(2));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2003: "), new DoubleWritable(3));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2004: "), new DoubleWritable(4));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2005: "), new DoubleWritable(5));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2006: "), new DoubleWritable(6));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2007: "), new DoubleWritable(7));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2008: "), new DoubleWritable(8));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2009: "), new DoubleWritable(9));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2010: "), new DoubleWritable(10));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2011: "), new DoubleWritable(11));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2012: "), new DoubleWritable(12));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2013: "), new DoubleWritable(13));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2014: "), new DoubleWritable(14));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2015: "), new DoubleWritable(15));
		mapDriver.withOutput(new Text("USA female unemployment rate with advanced education in 2016: "), new DoubleWritable(16));
		
		mapDriver.runTest();
	}

}
