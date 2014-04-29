package com.virtualeleven.triangles;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * The mapper class for enumerating the open triads (pairs of edges of the form {(A,B),(B,C)}
 * The mapper records each edge under its low degree member
 * Therefore the output key of the mapper will be the node with the lowest degree and the value would be the edge
 */
public class TriadFirstMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {

		String[] tokens = value.toString().split("\\|");

		String[] node1 = tokens[1].split("[\\(\\)]");
		String[] degreeToken1 = tokens[1].split("\\)");
		int degree1 = Integer.parseInt(degreeToken1[1]);

		String[] node2 = tokens[2].split("[()]");
		String[] degreeToken2 = tokens[2].split("\\)");
		int degree2 = Integer.parseInt(degreeToken2[1]);

		Text edge = new Text(tokens[0]);

		if (degree1 <= degree2) {
			output.collect(new Text(node1[1]), edge);
		} else {
			output.collect(new Text(node2[1]), edge);
		}
	}
}
