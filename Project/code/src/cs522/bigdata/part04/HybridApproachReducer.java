package cs522.bigdata.part04;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import cs522.bigdata.utils.Pair;

public class HybridApproachReducer extends
		Reducer<Pair, IntWritable, Text, Text> {

	// Initialization
	private MapWritable hf = new MapWritable();
	private String previousKey = "";
	private int totalFreq = 0;

	@Override
	public void reduce(Pair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		for (IntWritable value : values) {
			sum += value.get();
		}

		String keyPair = key.getKey().toString();
		String neighborPair = key.getValue().toString();

		//emit
		if (!previousKey.equals(keyPair)) {
			String result = "";
			int count = 0;
			for (Entry<Writable, Writable> valEntry : hf.entrySet()) {

				String keyItem = ((Text) valEntry.getKey()).toString();
				double valueItem = (double) ((IntWritable) valEntry.getValue())
						.get() / totalFreq;

				count++;
				if (count == hf.size())
					result += keyItem + " = " + valueItem;
				else
					result += keyItem + " = " + valueItem + " , ";
			}

			context.write(new Text(previousKey), new Text(result));

			hf.clear();
			totalFreq = 0;
		}

		totalFreq += sum;
		IntWritable newValue = hf.get(neighborPair) == null ? new IntWritable(
				sum) : new IntWritable(
				((IntWritable) hf.get(neighborPair)).get() + sum);
		hf.put(new Text(neighborPair), newValue);
		previousKey = keyPair;
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		String result = "";
		int count = 0;

		// emit last entry
		for (Entry<Writable, Writable> valEntry : hf.entrySet()) {
			String keyItem = ((Text) valEntry.getKey()).toString();
			double valueItem = (double) ((IntWritable) valEntry.getValue())
					.get() / totalFreq;

			count++;
			if (count == hf.size())
				result += keyItem + " = " + valueItem;
			else
				result += keyItem + " = " + valueItem + " , ";

		}

		context.write(new Text(previousKey), new Text(result));
		hf.clear();
	}
}