package cs522.labw2d1.prob2;


public class Simulator {

	public static void main(String[] args) {

		//Word Co-occurance Problem solution with Pair Approach  with m-mappers and r-reducers
		StripeApproach wc = new StripeApproach(2, 4,
				new String[] { "./input/RelFreqStripeinput1.txt", "./input/RelFreqStripeinput2.txt"});
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
