package cs522.lab2.prob2;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JobExecutor {

	public static void main(String[] args) {

		//WordCount with m-mappers and r-reducers
		WordCount wc = new WordCount(4, 3,
				new String[] { "./input/input11.txt", "./input/input12.txt", "./input/input13.txt", "./input/input14.txt" });
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
