package cs522.labw2d1.prob1;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.PairComparator;
import cs522.utils.Printer;

public class PairApproach {

	private int numberOfInputSplits;
	private int numberOfReducers;
	private String[] files;

	public PairApproach(int m, int r, String[] files) {
		this.numberOfInputSplits = m;
		this.numberOfReducers = r;
		this.files = files;
	}

	public PairApproach() {

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

		List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>> allMappedPairs = new ArrayList<>();

		// map step: input to key value pairs with m-mappers
		Path file;
		PairApproachMapper mapper = new PairApproachMapper();
		for (int i = 0; i < numberOfInputSplits; i++) {

			System.out.println("\n____________Mapper " + i + " input_____________\n");
			file = Paths.get(files[i]);
			
			List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> list = mapper.beginMapper(file);
			System.out.println("\n_____________Mapper " + i + " Output_____________\n");
			allMappedPairs.add(list);
			Printer.printListOfKeyValuePairs(list);
		}

		// shuffle
		List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>> partitionedPairs = shuffle(allMappedPairs);

		// sort and prepare for reducer input
		List<List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>>> reducerInputs = new ArrayList<>();
		PairApproachReducer reducer = new PairApproachReducer();
		for (int i = 0; i < numberOfReducers; i++) {

			List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>> reducerInput = reducer.combine(partitionedPairs.get(i));
			System.out.println("\n_____________Reducer " + i + " Input_____________\n");
			reducerInputs.add(reducerInput);
			Printer.printListOfGroupByPair(reducerInput);

		}

		// reduce
		for (int i = 0; i < numberOfReducers; i++) {

			List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>> reducerInput = reducerInputs.get(i);
			System.out.println("\n_____________Reducer " + i + " Output_____________\n");
			List<KeyValuePair<KeyValuePair<Integer, Integer>, Double>> reducerOutput = reducer.beginReducer(reducerInput);
			Printer.printListOfKeyValuePairs(reducerOutput);
		}

	}

	private List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>> shuffle(
			List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>> allMappedPairs) {

		List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>> partitionedPairs = new ArrayList<>();
		List<List<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>>> shuffledKeys = new ArrayList<>();

		//initialize m * r matrix for shuffle
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			shuffledKeys.add(new ArrayList<List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>>());
		}
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			for (int j = 0; j < this.numberOfReducers; j++) {
				shuffledKeys.get(i).add(new ArrayList<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>());
			}
		}

		for (int i = 0; i < this.numberOfReducers; i++) {
			partitionedPairs.add(new ArrayList<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>>());
		}

		// shuffle step
		int i = 0;
		for (List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> list : allMappedPairs) {
			for (KeyValuePair<KeyValuePair<Integer, Integer>, Integer> pair : list) {
				int partitionLevel = getPartition(pair.getKey().getKey());

				shuffledKeys.get(i).get(partitionLevel).add(pair);
				partitionedPairs.get(partitionLevel).add(pair);
			}

			i++;

		}
		PairComparator<Integer, Integer, Integer> comparator = new PairComparator<>();
		for (int j = 0; j < this.numberOfInputSplits; j++) {
			for (int k = 0; k < this.numberOfReducers; k++) {
				System.out.println("\n________Pairs sent from Mapper " + j + " to Reducer " + k + "__________\n");
				if (shuffledKeys.size() > j && shuffledKeys.get(j).size() > k) {
					List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> partitionedList = shuffledKeys.get(j).get(k);
					comparator.sort(partitionedList);
					for (KeyValuePair<KeyValuePair<Integer, Integer>, Integer> keyVal : partitionedList)
						System.out.println("<" + keyVal.getKey() + "," + keyVal.getValue() + ">");
				}
			}
		}

		return partitionedPairs;
	}

}
