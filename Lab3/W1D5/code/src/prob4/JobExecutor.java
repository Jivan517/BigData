package prob4;


public class JobExecutor {

	public static void main(String[] args) {

		//Word Co-occurance Problem solution with Pair Approach  with m-mappers and r-reducers
		StripeApproach wc = new StripeApproach(2, 4,
				new String[] { "./input/input1.txt", "./input/input2.txt"});
		System.out.println("Number of Input-Splits: " + wc.getNumberOfInputSplits());
		System.out.println("Number of Reducers: " + wc.getNumberOfReducers());
		wc.submitJob();

	}

}
