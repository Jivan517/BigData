package cs522.labw2d1.prob3;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.PairComparator;
import cs522.utils.Printer;

public class SecondarySort {

	private int numberOfInputSplits;
	private int numberOfReducers;
	private String[] files;

	public SecondarySort(int m, int r, String[] files) {
		this.numberOfInputSplits = m;
		this.numberOfReducers = r;
		this.files = files;
	}

	public SecondarySort() {

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

	public int getPartition(String key) {
		return (int) key.hashCode() % numberOfReducers;
	}

	public void submitJob() {

		List<List<KeyValuePair<KeyValuePair<String, String>, String>>> allMappedPairs = new ArrayList<>();

		// map step: input to key value pairs with m-mappers
		Path file;
		SecondarySortMapper mapper = new SecondarySortMapper();
		for (int i = 0; i < numberOfInputSplits; i++) {

			System.out.println("\n____________Mapper " + i + " input_____________\n");
			file = Paths.get(files[i]);

			List<KeyValuePair<KeyValuePair<String, String>, String>> list = mapper.beginMapper(file);
			System.out.println("\n_____________Mapper " + i + " Output_____________\n");
			allMappedPairs.add(list);
			Printer.printListOfKeyValuePairs(list);
		}

		// shuffle
		List<List<KeyValuePair<KeyValuePair<String, String>, String>>> partitionedPairs = shuffle(allMappedPairs);

		// sort and prepare for reducer input
		List<List<GroupByPair<KeyValuePair<String, String>, String>>> reducerInputs = new ArrayList<>();
		SecondarySortReducer reducer = new SecondarySortReducer();
		for (int i = 0; i < numberOfReducers; i++) {

			List<GroupByPair<KeyValuePair<String, String>, String>> reducerInput = reducer
					.combine(partitionedPairs.get(i));
			System.out.println("\n_____________Reducer " + i + " Input_____________\n");
			reducerInputs.add(reducerInput);
			Printer.printListOfGroupByPair(reducerInput);

		}

		// reduce
		for (int i = 0; i < numberOfReducers; i++) {

			List<GroupByPair<KeyValuePair<String, String>, String>> reducerInput = reducerInputs.get(i);
			System.out.println("\n_____________Reducer " + i + " Output_____________\n");
			List<KeyValuePair<String, String>> reducerOutput = reducer
					.beginReducer(reducerInput);
			Printer.printListOfKeyValuePair(reducerOutput);
		}

	}

	private List<List<KeyValuePair<KeyValuePair<String, String>, String>>> shuffle(
			List<List<KeyValuePair<KeyValuePair<String, String>, String>>> allMappedPairs) {

		List<List<KeyValuePair<KeyValuePair<String, String>, String>>> partitionedPairs = new ArrayList<>();
		List<List<List<KeyValuePair<KeyValuePair<String, String>, String>>>> shuffledKeys = new ArrayList<>();

		// initialize m * r matrix for shuffle
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			shuffledKeys.add(new ArrayList<List<KeyValuePair<KeyValuePair<String, String>, String>>>());
		}
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			for (int j = 0; j < this.numberOfReducers; j++) {
				shuffledKeys.get(i).add(new ArrayList<KeyValuePair<KeyValuePair<String, String>, String>>());
			}
		}

		for (int i = 0; i < this.numberOfReducers; i++) {
			partitionedPairs.add(new ArrayList<KeyValuePair<KeyValuePair<String, String>, String>>());
		}
		

		// shuffle step
		int i = 0;
		for (List<KeyValuePair<KeyValuePair<String, String>, String>> list : allMappedPairs) {
			for (KeyValuePair<KeyValuePair<String, String>, String> pair : list) {
				int partitionLevel = getPartition(pair.getKey().getKey());

				shuffledKeys.get(i).get(partitionLevel).add(pair);
				partitionedPairs.get(partitionLevel).add(pair);
			}

			i++;

		}

		PairComparator<String, String, String> comparator = new PairComparator<>();
		for (int j = 0; j < this.numberOfInputSplits; j++) {
			for (int k = 0; k < this.numberOfReducers; k++) {
				System.out.println("\n________Pairs sent from Mapper " + j + " to Reducer " + k + "__________\n");
				if (shuffledKeys.size() > j && shuffledKeys.get(j).size() > k) {
					List<KeyValuePair<KeyValuePair<String, String>, String>> partitionedList = shuffledKeys.get(j)
							.get(k);
					comparator.sortDescAsc(partitionedList);
					Printer.printListOfKeyValuePairs(partitionedList);
				}
			}
		}

		return partitionedPairs;
	}

}
