package cs522.labw2d2.prob1;

public class Simulator {

	public static void main(String[] args) {

		// Word Co-occurance Problem solution with Pair Approach with m-mappers
		// and r-reducers
		InvertedIndex wc = new InvertedIndex(4, 3, new String[] { "./input/input1.txt", "./input/input2.txt",
				"./input/input3.txt", "./input/input4.txt" });
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
