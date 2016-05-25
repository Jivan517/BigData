package cs522.lab2;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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

		List<List<KeyValuePair<String, Integer>>> allMappedPairs = new ArrayList<>();
		int[] a = new int[4];

		// map step input to key value pairs with m-mappers
		Path file;
		for (int i = 0; i < numberOfInputSplits; i++) {

			System.out.println("\n____________Mapper " + i +" input_____________\n");
			file = Paths.get(files[i]);
			
			WordCountMapper mapper = new WordCountMapper(file);
			List<KeyValuePair<String, Integer>> list = mapper.processWordCount();
			System.out.println("\n_____________Mapper " + i + " Output_____________\n");
			allMappedPairs.add(list);
			printListOfKeyValuePair(list);
		}

		//shuffle
		List<List<KeyValuePair<String, Integer>>> partitionedPairs = shuffle(allMappedPairs);

		//reduce
		List<List<GroupByPair<String, Integer>>> reducerInputs = new ArrayList<List<GroupByPair<String, Integer>>>();
		for (int i = 0; i < numberOfReducers; i++) {

			//prepare reducer input
			WordCountReducer reducer = new WordCountReducer();
			List<GroupByPair<String, Integer>> reducerInput = reducer.combine(partitionedPairs.get(i));
			System.out.println("\n_____________Reducer " + i + " Input_____________\n");
			printListOfGroupByPair(reducerInput);

			reducerInputs.add(reducerInput);
			
			
		}
		
		for(int i = 0; i < numberOfReducers; i++){
			
			WordCountReducer reducer = new WordCountReducer();
			List<GroupByPair<String, Integer>> reducerInput = reducerInputs.get(i);
			System.out.println("\n_____________Reducer " + i + " Output_____________\n");
			List<KeyValuePair<String, Integer>> reducerOutput = reducer.wordCountReduce(reducerInput);
			printListOfKeyValuePair(reducerOutput);
		}

	}

	private List<List<KeyValuePair<String, Integer>>> shuffle(
			List<List<KeyValuePair<String, Integer>>> allMappedPairs) {
		List<List<KeyValuePair<String, Integer>>> partitionedPairs = new ArrayList<List<KeyValuePair<String, Integer>>>();

		for(int i = 0; i < this.numberOfReducers; i++){
			partitionedPairs.add(new ArrayList<KeyValuePair<String, Integer>>());
		}
		
		// shuffle  step
		for (List<KeyValuePair<String, Integer>> list : allMappedPairs) {
			for (KeyValuePair<String, Integer> pair : list) {
				int partitionLevel = getPartition(pair.getKey());

				partitionedPairs.get(partitionLevel).add(pair);
			}

		}
		return partitionedPairs;
	}

	private static void printListOfKeyValuePair(List<KeyValuePair<String, Integer>> list) {
		if (list != null) {
			for (KeyValuePair<String, Integer> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	private static void printListOfGroupByPair(List<GroupByPair<String, Integer>> list) {
		if (list != null) {
			for (GroupByPair<String, Integer> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValues() + ">");

			}
		}
	}

	

}
