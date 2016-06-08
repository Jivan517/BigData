package cs522.bigdata.part03;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeApproachMapper extends
		Mapper<Object, Text, Text, MapWritable> {

	// Initialization
	private MapWritable h = new MapWritable();
	private Text result = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context) {

		String[] values = value.toString().split("\\s+");

		// for each term
		for (int i = 0; i < values.length; i++) {

			// Initialize the stripe
			MapWritable stripe = (MapWritable) (h.get(new Text(values[i].trim()
					.toLowerCase())) == null ? new MapWritable() : h
					.get(new Text(values[i].trim().toLowerCase())));
			
			result.set(values[i]);

			// for each neighbor
			for (int j = i + 1; j < values.length
					&& !(values[i].trim().toLowerCase().equals(values[j].trim()
							.toLowerCase())); j++) {

				IntWritable stripeValue = stripe.get(new Text(values[j].trim()
						.toLowerCase())) == null ? one : new IntWritable(
						((IntWritable) stripe.get(new Text(values[j].trim()
								.toLowerCase()))).get() + 1);

				stripe.put(new Text(values[j].trim().toLowerCase()),
						stripeValue);

			}

			if(stripe.size() > 0)
				h.put(new Text(values[i]), stripe);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {

		// Emit
		for (Entry<Writable, Writable> entry : h.entrySet()) {

			Text key = (Text) entry.getKey();
			MapWritable value = (MapWritable) entry.getValue();
			context.write(key, value);
		}
	}
}
