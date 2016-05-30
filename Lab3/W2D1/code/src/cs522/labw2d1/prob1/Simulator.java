package cs522.labw2d1.prob1;


public class Simulator {

	public static void main(String[] args) {

		//Word Co-occurance Problem solution with Pair Approach  with m-mappers and r-reducers
		PairApproach wc = new PairApproach(2, 4,
				new String[] { "./input/RelFreqPairinput1.txt", "./input/RelFreqPairinput2.txt"});
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
