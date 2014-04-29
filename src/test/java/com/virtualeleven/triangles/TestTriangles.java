/**
 *
 */
package com.virtualeleven.triangles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests using mrunit framework
 */
public class TestTriangles {

    Mapper<Object, Text, Text, Text> mapper;
    Reducer<Text, Text, Text, Text> reducer;
    Reducer<Text, Text, Text, Text> reducer2;

    Mapper<Object, Text, Text, Text> mapper3 = new TriadFirstMapper();
    Reducer<Text, Text, Text, Text> combiner = new TriadFirstCombiner();
    Reducer<Text, Text, Text, Text> reducer3 = new TriadFirstReducer();

    @Before
    public void setUp() {
        mapper = new AugmentDegreeMapper();
        reducer = new AugmentDegreeFirstReducer();
        reducer2 = new AugmentDegreeSecondReducer();
    }

    @Test
    public void testMapper() throws Exception {
        MapDriver<Object, Text, Text, Text> mapDriver = new MapDriver<Object, Text, Text, Text>();
        mapDriver.setMapper(mapper);

        mapDriver.withInput(new LongWritable(1), new Text("Joe,Ethel"));
        mapDriver.withOutput(new Text("Joe"), new Text("Joe,Ethel"));
        mapDriver.withOutput(new Text("Ethel"), new Text("Joe,Ethel"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        ReduceDriver<Text, Text, Text, Text> reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer);

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("Joe,Ethel"));
        reduceDriver.withInput(new Text("Joe"), values);
        reduceDriver.withInput(new Text("Ethel"), values);
        reduceDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Joe)1"));
        reduceDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Ethel)1"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReducer() throws Exception {
        MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver = new MapReduceDriver<Object, Text, Text, Text, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

        mapReduceDriver.withInput(new LongWritable(1), new Text("Joe,Ethel"));
        mapReduceDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Ethel)1"));
        mapReduceDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Joe)1"));

        mapReduceDriver.runTest();
    }


    @Test
    public void testSecondReducer() throws Exception {
        ReduceDriver<Text, Text, Text, Text> reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer2);
        reduceDriver.withInput(new Text("Joe,Ethel"), Arrays.asList(new Text("Joe,Ethel|degree(Ethel)1"), new Text("Joe,Ethel|degree(Joe)1")));
        reduceDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Ethel)1|degree(Joe)1"));
        reduceDriver.runTest();
    }

    /**
     * Joe,Ethel
     * Ted,Ethel
     */
    @Test
    public void testPipeline() throws Exception {
        PipelineMapReduceDriver<Object, Text, Text, Text> pipelineDriver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
        pipelineDriver.addMapReduce(mapper, reducer);
        pipelineDriver.addMapReduce(new IdentityMapper<Object, Text>(), reducer2);

        pipelineDriver.withInput(new LongWritable(1), new Text("Joe,Ethel"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ted,Ethel"));

        pipelineDriver.withOutput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Ethel)2|degree(Joe)1"));
        pipelineDriver.withOutput(new Text("Ted,Ethel"), new Text("Ted,Ethel|degree(Ethel)2|degree(Ted)1"));

        pipelineDriver.runTest();
    }


    @Test
    public void testTriadFirstMapper() throws Exception {
        MapDriver<Object, Text, Text, Text> mapDriver = new MapDriver<Object, Text, Text, Text>();
        mapDriver.setMapper(mapper3);

        mapDriver.withInput(new Text("Joe,Ethel"), new Text("Joe,Ethel|degree(Ethel)2|degree(Joe)1"));
        mapDriver.withInput(new Text("Ted,Ethel"), new Text("Ted,Ethel|degree(Ethel)2|degree(Ted)1"));

        mapDriver.withOutput(new Text("Joe"), new Text("Joe,Ethel"));
        mapDriver.withOutput(new Text("Ted"), new Text("Ted,Ethel"));

        mapDriver.runTest();
    }

    @Test
    public void testCombiner() throws Exception {
        ReduceDriver<Text, Text, Text, Text> combinerDriver = ReduceDriver.newReduceDriver(combiner);

        combinerDriver.withInput(new Text("Joe"), Arrays.asList(new Text("Joe,Ethel")));
        combinerDriver.withInput(new Text("Ted"), Arrays.asList(new Text("Ted,Ethel")));
        combinerDriver.withInput(new Text("Lucy"), Arrays.asList(new Text("Ethel,Lucy"), new Text("Lucy,Fred")));
        combinerDriver.withOutput(new Text("Lucy"), new Text("Ethel,Lucy"));
        combinerDriver.withOutput(new Text("Lucy"), new Text("Lucy,Fred"));
        combinerDriver.runTest();
    }

    @Test
    public void testFirstReducer() throws Exception {
        ReduceDriver<Text, Text, Text, Text> reducer3Driver = ReduceDriver.newReduceDriver(reducer3);
        reducer3Driver.withInput(new Text("Lucy"), Arrays.asList(new Text("Ethel,Lucy"), new Text("Lucy,Fred")));
        reducer3Driver.withOutput(new Text("Ethel,Fred"), new Text("|Ethel,Lucy|Lucy,Fred"));
        reducer3Driver.runTest();
    }

    /**
     * Joe,Ethel
     * Ted,Ethel
     * Ethel,Randy
     * Randy,Ricky
     * Ethel,Fred
     * Ethel,Lucy
     * Ricky,Fred
     * Fred,Lucy
     */
    @Test
    public void testPipeline2() throws Exception {
        PipelineMapReduceDriver<Object, Text, Text, Text> pipelineDriver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
        pipelineDriver.addMapReduce(mapper, reducer);
        pipelineDriver.addMapReduce(new IdentityMapper<Object, Text>(), reducer2);
        pipelineDriver.addMapReduce(mapper3, combiner);
        pipelineDriver.addMapReduce(new IdentityMapper<Object, Text>(), reducer3);

        pipelineDriver.withInput(new LongWritable(1), new Text("Joe,Ethel"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ted,Ethel"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ethel,Randy"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Randy,Ricky"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ethel,Fred"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ethel,Lucy"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Ricky,Fred"));
        pipelineDriver.withInput(new LongWritable(1), new Text("Fred,Lucy"));

        pipelineDriver.withOutput(new Text("Ethel,Fred"), new Text("|Ethel,Lucy|Fred,Lucy"));
        pipelineDriver.withOutput(new Text("Ethel,Ricky"), new Text("|Ethel,Randy|Randy,Ricky"));
        pipelineDriver.runTest();
        pipelineDriver.runTest();
    }


}
