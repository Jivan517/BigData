package cs522.bigdata.part04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HybridApproach {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HybridApproach Job");
		
		// configuration should contain reference to your namenode
		//FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		//fs.delete(new Path("output/hybridapproach"), true);
		
		job.setJarByClass(HybridApproach.class);
		job.setMapperClass(HybridApproachMapper.class);
		job.setMapOutputKeyClass(cs522.bigdata.utils.Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(HybridApproachReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//FileInputFormat.addInputPath(job, new Path("input/wordcount/file01.txt"));
		//FileOutputFormat.setOutputPath(job, new Path("output/hybridapproach"));
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}
}
