package cs522.bigdata.part01;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()){
			word.set(itr.nextToken().replace("(", "").replace(")", ""));
			context.write(word, one);

		}
	}
	
}
