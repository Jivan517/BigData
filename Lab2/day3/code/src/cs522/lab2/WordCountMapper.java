package cs522.lab2;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import cs522.lab2.utils.KeyValuePair;
import cs522.lab2.utils.WordCountComparator;

public class WordCountMapper {

	private Path file;

	public WordCountMapper(Path file) {
		this.file = file;
	}

	public List<KeyValuePair<String, Integer>> processWordCount() {

		List<KeyValuePair<String, Integer>> combinedList = new ArrayList<>();
		if (Files.exists(file) && Files.isReadable(file)) {

			try {

				// File reader
				BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());
				

				// read each line
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					String[] words = line.split("\\s+");
					for (String word : words) {
						if (word.trim() != "") {
							List<KeyValuePair<String, Integer>> processedWords = map(word);
							if (!processedWords.isEmpty()) {
								combinedList.addAll(processedWords);
							}
						}
					}

				}

				reader.close();

				// Sort the mapped dict as per keys
				// WordCountComparator<String, Integer> comparator = new
				// WordCountComparator<>();
				// return comparator.sort(combinedList);
				//
				return combinedList;

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	public List<KeyValuePair<String, Integer>> map(String rawToken) {

		List<KeyValuePair<String, Integer>> list = new ArrayList<>();
		rawToken = rawToken.replace("\"", "");
		String[] splittedToken = rawToken.split("-");
		for (String token : splittedToken) {

			// check for valid word
			if (Pattern.matches("([a-zA-Z]+\\.)||([a-zA-Z]+)", token)) {
				token = token.replace(".", "");
				list.add(new KeyValuePair<String, Integer>(token, 1));
			}
		}

		return list;
	}
}
