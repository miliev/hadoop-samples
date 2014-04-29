/*
 *
 */
package com.virtualeleven.triangles;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class AugmentDegreeMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

	/**
	 * Input: Node1<tab>Node2
	 *        .....
	 *        NodeX<tab>NodeY
	 *
	 * Output: (Node1, (Node1, Node2))
	 *         (Node2, (Node1, Node2))
	 *         .....
	 *         (NodeX, (NodeX, NodeY))
	 *         (NodeY, (NodeX, NodeY))
	 **/
	@Override
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

        String[] tokens = value.toString().split(",");
        output.collect(new Text(tokens[0]), value);
        output.collect(new Text(tokens[1]), value);
	}
}
