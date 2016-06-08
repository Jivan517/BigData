package cs522.bigdata.part03;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeApproachReducer extends
		Reducer<Text, MapWritable, Text, Text> {
	
	

	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

		int totalFreq = 0;
		MapWritable hf = new MapWritable();

		for (MapWritable map : values) {
			/*
			 * if(hf.size() < 1) hf.putAll(map); else{
			 */
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				Text keyItem = (Text) entry.getKey();
				IntWritable valueItem = (IntWritable) entry.getValue();
				totalFreq += valueItem.get();

				IntWritable newValue = hf.get(keyItem) == null ? valueItem
						: new IntWritable(((IntWritable) hf.get(keyItem)).get()
								+ valueItem.get());
				hf.put(keyItem, newValue);

				// }
			}

		}
		
		for (Entry<Writable, Writable> entry : hf.entrySet()) {
			Text keyItem = (Text) entry.getKey();
			IntWritable valueItem = (IntWritable) entry.getValue();

			hf.put(keyItem, new DoubleWritable((double) valueItem.get()/totalFreq));

		}
		
		//emit
		int count = 0;
		String mappedValue = "";
		for (Entry<Writable, Writable> entry : hf.entrySet()) {
			Text keyItem = (Text) entry.getKey();
			DoubleWritable valueItem = (DoubleWritable) entry.getValue();
			
			count++;
			if(count == hf.size())
				mappedValue += keyItem.toString() + " = " + valueItem.get();
			else
				mappedValue += keyItem.toString() + " = " + valueItem.get() + " , ";

		}
		
		context.write(key, new Text("{" + mappedValue + "}"));
		
	}
}
