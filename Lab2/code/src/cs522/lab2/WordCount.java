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

	


	public static void main(String[] args) {
		Path file = Paths.get("./input/testDataForW1D1.txt");

		System.out.println("_____________Mapper Output_____________\n");
		WordCountMapper mapper = new WordCountMapper();
		List<KeyValuePair<String, Integer>> list = mapper.processWordCount(file);
		printListOfKeyValuePair(list);
		
		WordCountReducer reducer = new WordCountReducer();
		List<GroupByPair<String, Integer>> combinerOutput = reducer.combine(list);
		System.out.println("_____________Combiner Output_____________\n");
		printListOfGroupByPair(combinerOutput);
		
		System.out.println("_____________Reducer Output_____________\n");
		List<KeyValuePair<String, Integer>> reducerOutput = reducer.wordCountReduce(combinerOutput);
		printListOfKeyValuePair(reducerOutput);
		
		
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
