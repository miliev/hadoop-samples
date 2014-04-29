package com.virtualeleven.triangles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



 /**
  * the type parameters are the input keys type, the input values type, the
  * output keys type, the output values type
  * The reducer outputs the key, value pair where the key is the outer vertices  of the triad associated with it
  * for example, if the input to the reducer is <2, {1,2}> and <2,{2,3}>, then the following reducer will output < {1,3},<{1,2},{2,3}>
  * the output of this reducer can be used as a partial input to the mapper and the reducer to enumerate triangles
  */
public class TriadFirstReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String concatVal = "";
		Set<String> nodeSet = new HashSet<String>();
		while (values.hasNext()) {
			Text val = values.next();
			String[] valTokens = val.toString().split(",");

			for (int i = 0; i < valTokens.length; i++) {
				nodeSet.add(valTokens[i]);
			}

			concatVal += "|" + val;
		}

		nodeSet.remove(key.toString());
		String nodesKey = "";

		for (String node : nodeSet) {
			nodesKey += node + ",";
		}

		// fix this !!
		nodesKey = nodesKey.substring(0, nodesKey.length() - 1);

		output.collect(new Text(nodesKey), new Text(concatVal));
	}

}
