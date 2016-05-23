package cs522.lab1;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.*;

public class WordCount {

	public static void main(String[] args) {

		Path file = Paths.get("./input/testDataForW1D1.txt");

		WordCount wc = new WordCount();
		List<Map<String, Integer>> list = wc.mapper(file);
		wc.printData(list);
	}

	private List<Map<String, Integer>> mapper(Path file) {

		List<Map<String, Integer>> combinedList = new ArrayList<>();
		if (Files.exists(file) && Files.isReadable(file)) {

			try {

				// File reader
				BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());

				// read each line
				String line;
				while ((line = reader.readLine()) != null) {

					String[] words = line.split("\\s+");
					for (String word : words) {
						if (word.trim() != "") {
							List<Map<String, Integer>> processedWords = processToken(word);
							if (!processedWords.isEmpty()) {
								combinedList.addAll(processedWords);
							}
						}
					}

				}

				reader.close();

				// Sort the mapped dict as per keys
				WordCountUtil<String, Integer> comparator = new WordCountUtil<>();
				return comparator.sort(combinedList);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	private List<Map<String, Integer>> processToken(String rawToken) {

		List<Map<String, Integer>> list = new ArrayList<>();
		rawToken = rawToken.replace("\"", "");
		String[] splittedToken = rawToken.split("-");
		for (String token : splittedToken) {

			// check for valid word
			if (Pattern.matches("([a-zA-Z]+\\.)||([a-zA-Z]+)", token)) {

				token = token.replace(".", "");
				Map<String, Integer> map = new HashMap<String, Integer>();
				map.put(token, 1);
				list.add(map);
			}
		}

		return list;
	}

	private void printData(List<Map<String, Integer>> list) {
		if (list != null) {
			for (Map<String, Integer> dict : list) {
				System.out.println(dict);
			}
		}
	}

}
