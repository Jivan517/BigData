package cs522.labw2d1.prob3;

public class Simulator {

	public static void main(String[] args) {

		// Word Co-occurance Problem solution with Pair Approach with m-mappers
		// and r-reducers
		SecondarySort wc = new SecondarySort(3, 2, new String[] { "./input/SecondarySortingInput1.txt",
				"./input/SecondarySortingInput2.txt", "./input/SecondarySortingInput3.txt" });
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
