package cs522.labw2d2.prob1;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cs522.utils.KeyValuePair;

public class InvertedIndexMapper {

	public InvertedIndexMapper() {

	}

	public List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> beginMapper(Path file) {

		List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> combinedList = new ArrayList<>();
		if (Files.exists(file) && Files.isReadable(file)) {

			try {

				// File reader
				BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());

				int docId = Integer.MIN_VALUE;
				String firstLine = reader.readLine();
				System.out.println(firstLine);
				Pattern p = Pattern.compile("-?\\d+");
				Matcher m = p.matcher(firstLine);
				while (m.find()) {
					docId = Integer.parseInt(m.group());
				}

				// read each line
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);

					String[] lineContents = line.split("\\s+");
					for (int i = 0; i < lineContents.length; i++) {

						String data = lineContents[i];
						if (data.trim() != "") {

							List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> processedData = map(data, docId);
							if (!processedData.isEmpty()) {
								combinedList.addAll(processedData);
							}
						}
					}

				}

				reader.close();
				
				
				// In-Mapper combiner
				List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> inMapperCombinedList = new ArrayList<>();
				for (KeyValuePair<KeyValuePair<String, Integer>, Integer> pair : combinedList) {
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

	public List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> map(String rawToken, int docId) {

		List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> list = new ArrayList<>();
		rawToken = rawToken.replace("\"", "");
		String[] splittedToken = rawToken.split("-");
		for (String token : splittedToken) {

			// check for valid word
			if (Pattern.matches("([a-zA-Z]+\\.)||([a-zA-Z]+)", token)) {
				token = token.replace(".", "");
				list.add(new KeyValuePair<KeyValuePair<String, Integer>, Integer>(
						new KeyValuePair<>(token.toLowerCase(), docId), 1));
			}
		}

		return list;
	}
}
