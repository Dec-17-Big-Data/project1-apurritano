package com.revature.test;

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
	public void testMapper(){
		mapDriver.withInput(new LongWritable(), new Text("\"Argentina\",\"ARG\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"10.29958\",\"8.60549\",\"13.46936\",\"14.3721\",\"\",\"15.76742\",\"13.72042\",\"14.12948\",\"14.65367\",\"\",\"\",\"14.09986\",\"13.82683\",\"13.71856\",\"\",\"\","));
		
		mapDriver.withOutput(new Text("Argentina"), new DoubleWritable(13.71856));
		
		mapDriver.runTest();
		
	}
	
	@Test
	public void testReducer(){
		//write test
	}
	
	@Test
	public void testMapReduce(){
		//write test
	}

}
