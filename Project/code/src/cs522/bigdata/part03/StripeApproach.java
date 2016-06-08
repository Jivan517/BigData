package cs522.bigdata.part03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeApproach {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StripeApproach Job");
		
		// configuration should contain reference to your namenode
		//FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		//fs.delete(new Path("output/stripeapproach"), true);

		job.setJarByClass(StripeApproach.class);
		job.setMapperClass(StripeApproachMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setReducerClass(StripeApproachReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		FileInputFormat.addInputPath(job,
//				new Path("input/wordcount/file01.txt"));
//		FileOutputFormat.setOutputPath(job, new Path("output/stripeapproach"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
