package cs522.bigdata.part02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairApproach {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PairApproach Job");

		// configuration should contain reference to your namenode
		//FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		//fs.delete(new Path("output/pairapproach"), true);
		
		job.setJarByClass(PairApproach.class);
		job.setMapperClass(PairApproachMapper.class);
		job.setMapOutputKeyClass(cs522.bigdata.utils.Pair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(PairApproachReducer.class);
		job.setOutputKeyClass(cs522.bigdata.utils.Pair.class);
		job.setOutputValueClass(DoubleWritable.class);

		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		FileInputFormat.addInputPath(job,
//				new Path("input/wordcount/file01.txt"));
//		FileOutputFormat.setOutputPath(job, new Path("output/pairapproach"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
