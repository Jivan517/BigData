package cs522.lab2;

import java.nio.file.Path;
import java.nio.file.Paths;

public class JobExecutor {

	public static void main(String[] args) {

		//WordCount with m-mappers and r-reducers
		WordCount wc = new WordCount(3, 4,
				new String[] { "./input/input1.txt", "./input/input2.txt", "./input/input3.txt" });
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
