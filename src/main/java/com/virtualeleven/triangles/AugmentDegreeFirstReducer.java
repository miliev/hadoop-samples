package com.virtualeleven.triangles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// http://www.cse.usf.edu/~anda/CIS6930-S11/papers/graph-processing-w-mapreduce.pdf

/**
 * The reducer is used to aggregate the values associated with a particular key.
 * Input: (Joe,Ethel) . The case of s simple graph with only one edge
 *
 * Output: (("Joe,Ethel"), ("Joe,Ethel|degree(Joe)1")
 *         (("Joe,Ethel"), ("Joe,Ethel|degree(Ethel)1")
 *
 * The node is the input key and the values are the edges associated with the
 * node the degree of the node is calculated by iterating through the values and
 * counting the number of edges associated with the node.
 * The output of the
 * reducer is in the form of key, value pair where the key is the edge and the
 * value is the degree of one of the nodes in the edge
 */
public class AugmentDegreeFirstReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		int count = 0;

		// iterate through the edges associated with the vertex given as key
		// argument
		while (values.hasNext()) {
			list.add(values.next().toString());
		}

		count = list.size();
		for (String listVal : list) {
			output.collect(new Text(listVal), new Text(listVal + "|"
					+ "degree(" + key.toString() + ")" + count));

		}
	}

}
