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

import com.revature.Q3_Mapper;
import com.revature.Q3_Reducer;

public class Q3_Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		Q3_Mapper mapper = new Q3_Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q3_Reducer reducer = new Q3_Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		
		String str = "\"United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"69.302001953125\",\"68.802001953125\",\"69.0240020751953\",\"69.5419998168945\","
				+ "\"69.9820022583008\",\"70.1650009155273\",\"70.6579971313477\",\"70.9810028076172\",\"71.1549987792969\",\"71\","
				+ "\"70.1220016479492\",\"68.7910003662109\",\"67.9130020141602\",\"68.1800003051758\",\"68.5139999389648\",\"69.0100021362305\","
				+ "\"68.6940002441406\",\"67.463996887207\",\"63.5250015258789\",\"62.6809997558594\",\"62.9599990844727\",\"63.6580009460449\","
				+ "\"63.6969985961914\",\"64.2409973144531\",\"64.7129974365234\",\"64\",";
				
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("The percent of change in male employment from 2000 in United States is: "), new DoubleWritable(-7));
		
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
		
		String str = "\"Italy\",\"ITA\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"62.1619987487793\",\"60.2879981994629\",\"59.2700004577637\",\"57.6230010986328\",\"56.5800018310547\","
				+ "\"56.2630004882813\",\"56.0050010681152\",\"55.9440002441406\",\"56.0709991455078\",\"56\",\"56.484001159668\","
				+ "\"57.0009994506836\",\"57.382999420166\",\"57.8540000915527\",\"57.2290000915527\",\"57.4879989624023\",\"57.4770011901855\","
				+ "\"57.0579986572266\",\"55.5309982299805\",\"54.6180000305176\",\"54.2169990539551\",\"53.2569999694824\",\"51.6240005493164\","
				+ "\"51.3880004882813\",\"51.5859985351563\",\"51\",";
				
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("The percent of change in male employment from 2000 in Italy is: "), new DoubleWritable(-5));
		
		mapDriver.runTest();
	}

}

