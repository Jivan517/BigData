package cs522.bigdata.part02;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;

import cs522.bigdata.utils.Pair;

public class PairApproachReducer extends
		Reducer<Pair, IntWritable, Pair, DoubleWritable> {

	private int totalFreq = 1;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}

		if (key.getValue().toString().equals("*")) {
			totalFreq = sum;
		} else {

			context.write(key, new DoubleWritable((double) sum / totalFreq));

		}

	}
}
