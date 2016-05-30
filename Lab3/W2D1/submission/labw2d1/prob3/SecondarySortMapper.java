package cs522.labw2d1.prob3;

import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import cs522.utils.KeyValuePair;

public class SecondarySortMapper {

	public SecondarySortMapper(){
		
	}

	public List<KeyValuePair<KeyValuePair<String, String>, String>> beginMapper(Path file) {

		List<KeyValuePair<KeyValuePair<String, String>, String>> combinedList = new ArrayList<>();
		if (Files.exists(file) && Files.isReadable(file)) {

			try {

				// File reader
				BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset());

				// read each line
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					
					String[] lineContents = line.split("\\n");
					for (int i = 0; i < lineContents.length; i++) {

						String data = lineContents[i];
						if (data.trim() != "") {
							
							List<KeyValuePair<KeyValuePair<String, String>, String>> processedData = map(data);
							if (!processedData.isEmpty()) {
								combinedList.addAll(processedData);
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

	public List<KeyValuePair<KeyValuePair<String, String>, String>> map(String data) {

		List<KeyValuePair<KeyValuePair<String, String>, String>> list = new ArrayList<>();
		String[] values = data.split("\\t");
		
		if(values.length > 2){
		
			String sensorId = values[1];
			String timeStamp = values[0];
			String sensorData = values[2];
			
			list.add(new KeyValuePair<KeyValuePair<String, String>, String>(
					new KeyValuePair<>(sensorId, timeStamp), sensorData));
			
		}
		
		return list;
	}
}
