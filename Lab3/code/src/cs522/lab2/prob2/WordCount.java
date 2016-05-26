package cs522.lab2.prob2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import cs522.lab2.utils.GroupByPair;
import cs522.lab2.utils.KeyValuePair;
import cs522.lab2.utils.WordCountComparator;

public class WordCount {

	private int numberOfInputSplits;
	private int numberOfReducers;
	private String[] files;

	public WordCount(int m, int r, String[] files) {
		this.numberOfInputSplits = m;
		this.numberOfReducers = r;
		this.files = files;
	}

	public WordCount() {

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

		List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>> allMappedPairs = new ArrayList<>();

		// map step: input to key value pairs with m-mappers
		Path file;
		for (int i = 0; i < numberOfInputSplits; i++) {

			System.out.println("\n____________Mapper " + i + " input_____________\n");
			file = Paths.get(files[i]);

			WordCountMapper mapper = new WordCountMapper(file);
			List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> list = mapper.processWordCount();
			System.out.println("\n_____________Mapper " + i + " Output_____________\n");
			allMappedPairs.add(list);
			printListOfKeyValuePair(list);
		}

		/**/

		// shuffle
		List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>> partitionedPairs = shuffle(allMappedPairs);

		// sort and prepare for reducer input
		List<List<GroupByPair<Character, KeyValuePair<Integer, Integer>>>> reducerInputs = new ArrayList<List<GroupByPair<Character, KeyValuePair<Integer, Integer>>>>();
		for (int i = 0; i < numberOfReducers; i++) {

			WordCountReducer reducer = new WordCountReducer();
			List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> reducerInput = reducer.combine(partitionedPairs.get(i));
			System.out.println("\n_____________Reducer " + i + " Input_____________\n");
			printListOfGroupByPair(reducerInput);

			reducerInputs.add(reducerInput);

		}

		// reduce
		for (int i = 0; i < numberOfReducers; i++) {

			WordCountReducer reducer = new WordCountReducer();
			List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> reducerInput = reducerInputs.get(i);
			System.out.println("\n_____________Reducer " + i + " Output_____________\n");
			List<KeyValuePair<Character, Double>> reducerOutput = reducer.wordCountReduce(reducerInput);
			printList(reducerOutput);
		}

		/**/

	}

	private List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>> shuffle(
			List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>> allMappedPairs) {

		List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>> partitionedPairs = 
				new ArrayList<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>>();
		List<List<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>>> shuffledKeys = new ArrayList<>();

		for (int i = 0; i < this.numberOfInputSplits; i++) {
			shuffledKeys.add(new ArrayList<List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>>());
		}
		for (int i = 0; i < this.numberOfInputSplits; i++) {
			for (int j = 0; j < this.numberOfReducers; j++) {
				shuffledKeys.get(i).add(new ArrayList<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>());
			}
		}

		for (int i = 0; i < this.numberOfReducers; i++) {
			partitionedPairs.add(new ArrayList<KeyValuePair<Character, KeyValuePair<Integer, Integer>>>());
		}

		// shuffle step
		int i = 0;
		for (List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> list : allMappedPairs) {
			for (KeyValuePair<Character, KeyValuePair<Integer, Integer>> pair : list) {
				int partitionLevel = getPartition(pair.getKey().toString());

				shuffledKeys.get(i).get(partitionLevel).add(pair);
				partitionedPairs.get(partitionLevel).add(pair);
			}

			i++;

		}
		
		WordCountComparator<Character, KeyValuePair<Integer, Integer>> comparator = new WordCountComparator<>();
		for (int j = 0; j < this.numberOfInputSplits; j++) {
			for (int k = 0; k < this.numberOfReducers; k++) {
				System.out.println("\n________Pairs sent from Mapper " + j + " to Reducer " + k + "__________\n");
				if (shuffledKeys.size() > j && shuffledKeys.get(j).size() > k) {
					List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> partitionedList = shuffledKeys.get(j).get(k);
					comparator.sort(partitionedList);
					for (KeyValuePair<Character, KeyValuePair<Integer, Integer>> keyVal : partitionedList)
						System.out.println("<" + keyVal.getKey() + "," + keyVal.getValue() + ">");
				}
			}
		}

		return partitionedPairs;
	}

	private static void printListOfKeyValuePair(List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> list) {
		if (list != null) {
			for (KeyValuePair<Character, KeyValuePair<Integer, Integer>> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	private static void printListOfGroupByPair(List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> list) {
		if (list != null) {
			for (GroupByPair<Character, KeyValuePair<Integer, Integer>> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValues() + ">");

			}
		}
	}
	
	private static void printList(List<KeyValuePair<Character, Double>> list) {
		if (list != null) {
			for (KeyValuePair<Character, Double> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValue() + ">");

			}
		}
	}

}
