package cs522.lab3d5;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import cs522.utils.KeyValuePair;

public class PairApproachMapper {

	private Path file;

	public PairApproachMapper(Path file) {
		this.file = file;
	}

	public List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> processWordCount() {

		List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> combinedList = new ArrayList<>();
		if (Files.exists(file) && Files.isReadable(file)) {

			try {

				// File reader
				BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());

				// read each line
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					String[] numbers = line.split("\\s+");
					for (int i = 0; i < numbers.length; i++) {

						String number = numbers[i];
						if (number.trim() != "") {

							// sliding window
							String[] neighbourNumbers = Arrays.copyOfRange(numbers, i + 1, numbers.length);

							List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> processedWords = map(number,
									neighbourNumbers);
							if (!processedWords.isEmpty()) {
								combinedList.addAll(processedWords);
							}
						}
					}

				}

				reader.close();

				// In-Mapper combiner
				List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> inMapperCombinedList = new ArrayList<>();
				for (KeyValuePair<KeyValuePair<Integer, Integer>, Integer> pair : combinedList) {
					int length = inMapperCombinedList.size();
					if (length < 1) {
						inMapperCombinedList.add(pair);
					} else {

						boolean isFound = false;
						for (int i = 0; i < length; i++) {
							if (inMapperCombinedList.get(i).getKey().equals(pair.getKey())) {
								int value = inMapperCombinedList.get(i).getValue();
								inMapperCombinedList.get(i).setValue(value + pair.getValue());
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

	public List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> map(String number, String[] neighbours) {

		List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> list = new ArrayList<>();
		number = number.replace("\"", "").replace("'", "");
		for (String neighbour : neighbours) {

			
			// check for valid number
			if (Pattern.matches("([0-9]+)", neighbour) && Pattern.matches("([0-9]+)", number)) {

				int processingNumber = Integer.parseInt(number);
				int neighbourNumber = Integer.parseInt(neighbour);
				if(processingNumber == neighbourNumber)
					break;
				
				list.add(new KeyValuePair<KeyValuePair<Integer, Integer>, Integer>(
						new KeyValuePair<>(processingNumber, neighbourNumber), 1));
			}
		}

		return list;
	}
}
