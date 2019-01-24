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

import com.revature.Q4_Mapper;
import com.revature.Q4_Reducer;

public class Q4_Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		Q4_Mapper mapper = new Q4_Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
		
		Q4_Reducer reducer = new Q4_Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		String str = "\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"52.5610008239746\",\"52.5690002441406\",\"52.9469985961914\",\"54.1669998168945\","
				+ "\"54.6139984130859\",\"55.0340003967285\",\"55.7900009155273\",\"56.1259994506836\",\"56.4620018005371\",\"56\","
				+ "\"56.1189994812012\",\"55.2980003356934\",\"55.1599998474121\",\"55.0439987182617\",\"55.3009986877441\",\"55.7270011901855\","
				+ "\"55.7519989013672\",\"55.3969993591309\",\"53.5410003662109\",\"52.6469993591309\",\"52.1679992675781\",\"52.2569999694824\","
				+ "\"52.3470001220703\",\"52.693000793457\",\"53.1230010986328\",\"53\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("The percent of change in female employment from 2000 in United States is: "), new DoubleWritable(-3));
		
		mapDriver.runTest();
		
	}
	
	@Test
	public void testReducer(){
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(37));
		
		reduceDriver.withInput(new Text("Italy"), values);
		
		reduceDriver.withOutput(new Text("Italy"), new DoubleWritable(37));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		
		String str = "\"Italy\",\"ITA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"30.0860004425049\",\"29.4099998474121\",\"28.681999206543\",\"28.4699993133545\",\"28.2210006713867\","
				+ "\"28.5060005187988\",\"28.5809993743896\",\"28.8980007171631\",\"29.5510005950928\",\"30\",\"31.3799991607666\","
				+ "\"31.9969997406006\",\"32.640998840332\",\"34.2299995422363\",\"34.0209999084473\",\"34.6440010070801\",\"34.8040008544922\","
				+ "\"35.1399993896484\",\"34.4099998474121\",\"34.1699981689453\",\"34.2840003967285\",\"34.4900016784668\",\"33.8680000305176\","
				+ "\"33.9309997558594\",\"34.2970008850098\",\"34\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(str));
		
		mapDriver.withOutput(new Text("The percent of change in female employment from 2000 in Italy is: "), new DoubleWritable(4));
		
		mapDriver.runTest();
	}

}
