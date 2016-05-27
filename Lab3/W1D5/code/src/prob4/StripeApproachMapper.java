package prob4;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

import cs522.utils.KeyValuePair;

public class StripeApproachMapper {

	private Path file;

	public StripeApproachMapper(Path file) {
		this.file = file;
	}

	public List<KeyValuePair<Integer, Map<Integer, Integer>>> processWordCount() {

		List<KeyValuePair<Integer, Map<Integer, Integer>>> combinedList = new ArrayList<>();
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

							List<KeyValuePair<Integer, Map<Integer, Integer>>> processedWords = map(number,
									neighbourNumbers);
							if (!processedWords.isEmpty()) {
								combinedList.addAll(processedWords);
							}
						}
					}

				}

				reader.close();

				return combinedList;

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	public List<KeyValuePair<Integer, Map<Integer, Integer>>> map(String number, String[] neighbours) {

		List<KeyValuePair<Integer, Map<Integer, Integer>>> list = new ArrayList<>();
		number = number.replace("\"", "").replace("'", "");

		Map<Integer, Integer> neighbourMap = new HashMap<>();
		int processingNumber = Integer.parseInt(number);
		for (String neighbour : neighbours) {

			// check for valid number
			if (Pattern.matches("([0-9]+)", neighbour) && Pattern.matches("([0-9]+)", number)) {

				int neighbourNumber = Integer.parseInt(neighbour);
				if (processingNumber == neighbourNumber)
					break;

				neighbourMap.put(neighbourNumber,
						neighbourMap.get(neighbourNumber) == null ? 1 : neighbourMap.get(neighbourNumber) + 1);

			}
		}

		if(neighbours.length > 0)
			list.add(new KeyValuePair<Integer, Map<Integer, Integer>>(processingNumber, neighbourMap));
		return list;
	}
}
