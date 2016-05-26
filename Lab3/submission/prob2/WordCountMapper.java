package cs522.lab2.prob2;

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

	public List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> processWordCount() {

		List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> combinedList = new ArrayList<>();
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
							List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> processedWords = map(word);
							if (!processedWords.isEmpty()) {
								combinedList.addAll(processedWords);
							}
						}
					}

				}

				reader.close();

				// In-Mapper combiner
				List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> inMapperCombinedList = new ArrayList<>();
				for (KeyValuePair<Character, KeyValuePair<Integer, Integer>> pair : combinedList) {
					int length = inMapperCombinedList.size();
					if (length < 1) {
						inMapperCombinedList.add(pair);
					} else {

						boolean isFound = false;
						for (int i = 0; i < length; i++) {
							if (inMapperCombinedList.get(i).getKey().toString().toLowerCase()
									.equals(pair.getKey().toString().toLowerCase())) {

								KeyValuePair<Integer, Integer> keyVal = inMapperCombinedList.get(i).getValue();
								KeyValuePair<Integer, Integer> pairKeyVal = pair.getValue();
								inMapperCombinedList.get(i)
										.setValue(new KeyValuePair<Integer, Integer>(
												keyVal.getKey() + pairKeyVal.getKey(),
												keyVal.getValue() + pairKeyVal.getValue()));
								isFound = true;
								break;

							}

						}

						if (!isFound) {
							inMapperCombinedList.add(pair);
						}
					}

				}

				return inMapperCombinedList;

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	public List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> map(String rawToken) {

		List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> list = new ArrayList<>();
		rawToken = rawToken.replace("\"", "").replace("'", "");
		String[] splittedToken = rawToken.split("-");
		for (String token : splittedToken) {

			// check for valid word
			if (Pattern.matches("([a-zA-Z]+\\.)||([a-zA-Z]+)", token)) {
				token = token.replace(".", "");
				list.add(new KeyValuePair<Character, KeyValuePair<Integer, Integer>>(Character.toLowerCase(token.charAt(0)),
						new KeyValuePair<Integer, Integer>(token.length(), 1)));
			}
		}

		return list;
	}
}
