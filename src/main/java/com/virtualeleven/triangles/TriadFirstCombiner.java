package com.virtualeleven.triangles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * The combiner's task is to lower the data passed to reducers. In this case we
 * discard the vertices with degree <= 1 since they can not form a triangle
 */
public class TriadFirstCombiner extends MapReduceBase implements Reducer< Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// emit the keys that have a degree greater than 1
		ArrayList<String> list = new ArrayList<String>();

		int count = 0;
		while (values.hasNext()) {
			list.add(values.next().toString());
		}

		count = list.size();
		if (count > 1) {
			for (String listVal : list) {
				output.collect(key, new Text(listVal));
			}
		}
	}

}
