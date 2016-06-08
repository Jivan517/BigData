package cs522.bigdata.part04;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import cs522.bigdata.utils.Pair;

public class HybridApproachMapper extends Mapper<Object, Text, Pair, IntWritable> {

	private IntWritable one = new IntWritable(1);

	// Initialization for Associative Array
	MapWritable hashTable = new MapWritable();

	@Override
	public void map(Object key, Text value, Context context) {

		// read a record - line
		String[] words = value.toString().split("\\s+");

		// for each term in the record doc
		for (int i = 0; i < words.length; i++) {

			// for each neighbor term
			for (int j = i + 1; j < words.length
					&& !words[i].trim().toLowerCase()
							.equals(words[j].trim().toLowerCase()); j++) {

				Pair pair = new Pair(new Text(words[i].trim().toLowerCase()), new Text(words[j]
						.trim().toLowerCase()));
				IntWritable pairCount = hashTable.get(pair) != null ? new IntWritable(
						((IntWritable) hashTable.get(pair)).get() + one.get())
						: one;


				hashTable.put(pair, pairCount);
			}
		}

	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		// emit each pair
		for (Entry<Writable, Writable> pair : hashTable.entrySet()) {
			
			Pair key = (Pair) pair.getKey();
			IntWritable value = (IntWritable) pair.getValue();
			context.write(key, value);
		}
	}

}
