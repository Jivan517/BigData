package prob4;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.WordComparator;

public class StripeApproach {

	private int numberOfInputSplits;
	private int numberOfReducers;
	private String[] files;

	public StripeApproach(int m, int r, String[] files) {
		this.numberOfInputSplits = m;
		this.numberOfReducers = r;
		this.files = files;
	}

	public StripeApproach() {

	}

	public int getNumberOfInputSplits() {
		return numberOfInputSplits;
	}

	public void setNumberOfInputSplits(int numberOfInputSplits) {
		this.numberOfInputSplits = numberOfInputSplits;
	}

	public int getNumberOfReducers() {
		return numberOfReducers;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}

	public String[] getFiles() {
		return files;
	}

	public void setFiles(String[] files) {
		this.files = files;
	}

	public int getPartition(int key) {
		return (int) key % numberOfReducers;
	}

	public void submitJob() {

		List<List<KeyValuePair<Integer, Map<Integer, Integer>>>> allMappedPairs = new ArrayList<>();

		// map step: input to key value pairs with m-mappers
		Path file;
		for (int i = 0; i < numberOfInputSplits; i++) {

			System.out.println("\n____________Mapper " + i + " input_____________\n");
			file = Paths.get(files[i]);

			StripeApproachMapper mapper = new StripeApproachMapper(file);
			List<KeyValuePair<Integer, Map<Integer, Integer>>> list = mapper.processWordCount();
			System.out.println("\n_____________Mapper " + i + " Output_____________\n");
			allMappedPairs.add(list);
			printListOfKeyValuePair(list);
		}

		// shuffle
		List<List<KeyValuePair<Integer, Map<Integer, Integer>>>> partitionedPairs = shuffle(allMappedPairs);

		// sort and prepare for reducer input
		List<List<GroupByPair<Integer, Map<Integer, Integer>>>> reducerInputs = new ArrayList<>();
		for (int i = 0; i < numberOfReducers; i++) {

			StripeApproachReducer reducer = new StripeApproachReducer();
			List<GroupByPair<Integer, Map<Integer, Integer>>> reducerInput = reducer.combine(partitionedPairs.get(i));
			System.out.println("\n_____________Reducer " + i + " Input_____________\n");
			printListOfGroupByPair(reducerInput);

			reducerInputs.add(reducerInput);

		}

		// reduce
		for (int i = 0; i < numberOfReducers; i++) {

			StripeApproachReducer reducer = new StripeApproachReducer();
			List<GroupByPair<Integer, Map<Integer, Integer>>> reducerInput = reducerInputs.get(i);
			System.out.println("\n_____________Reducer " + i + " Output_____________\n");
			List<KeyValuePair<Integer, Map<Integer, Integer>>> reducerOutput = reducer.wordCountReduce(reducerInput);
			printListOfKeyValuePair(reducerOutput);
		}

	}

	private List<List<KeyValuePair<Integer, Map<Integer, Integer>>>> shuffle(
			List<List<KeyValuePair<Integer, Map<Integer, Integer>>>> allMappedPairs) {

		List<List<KeyValuePair<Integer, Map<Integer, Integer>>>> partitionedPairs = new ArrayList<>();
		List<List<List<KeyValuePair<Integer, Map<Integer, Integer>>>>> shuffledKeys = new ArrayList<>();

		//initialize m * r matrix for shuffle
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			shuffledKeys.add(new ArrayList<List<KeyValuePair<Integer, Map<Integer, Integer>>>>());
		}
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			for (int j = 0; j < this.numberOfReducers; j++) {
				shuffledKeys.get(i).add(new ArrayList<KeyValuePair<Integer, Map<Integer, Integer>>>());
			}
		}

		for (int i = 0; i < this.numberOfReducers; i++) {
			partitionedPairs.add(new ArrayList<KeyValuePair<Integer, Map<Integer, Integer>>>());
		}

		// shuffle step
		int i = 0;
		for (List<KeyValuePair<Integer, Map<Integer, Integer>>> list : allMappedPairs) {
			for (KeyValuePair<Integer, Map<Integer, Integer>> pair : list) {
				int partitionLevel = getPartition(pair.getKey());

				shuffledKeys.get(i).get(partitionLevel).add(pair);
				partitionedPairs.get(partitionLevel).add(pair);
			}

			i++;

		}
		WordComparator<Integer, Map<Integer, Integer>> comparator = new WordComparator<>();
		for (int j = 0; j < this.numberOfInputSplits; j++) {
			for (int k = 0; k < this.numberOfReducers; k++) {
				System.out.println("\n________Pairs sent from Mapper " + j + " to Reducer " + k + "__________\n");
				if (shuffledKeys.size() > j && shuffledKeys.get(j).size() > k) {
					List<KeyValuePair<Integer, Map<Integer, Integer>>> partitionedList = shuffledKeys.get(j).get(k);
					comparator.sort(partitionedList);
					for (KeyValuePair<Integer, Map<Integer, Integer>> keyVal : partitionedList)
						System.out.println("<" + keyVal.getKey() + "," + keyVal.getValue() + ">");
				}
			}
		}

		return partitionedPairs;
	}

	private static void printListOfKeyValuePair(List<KeyValuePair<Integer, Map<Integer, Integer>>> list) {
		if (list != null) {
			for (KeyValuePair<Integer, Map<Integer, Integer>> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	private static void printListOfGroupByPair(List<GroupByPair<Integer, Map<Integer, Integer>>> list) {
		if (list != null) {
			for (GroupByPair<Integer, Map<Integer, Integer>> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValues() + ">");

			}
		}
	}

}
