package cs522.lab2.prob2;

import java.util.*;

import cs522.lab2.utils.GroupByPair;
import cs522.lab2.utils.KeyValuePair;
import cs522.lab2.utils.WordCountComparator;

public class WordCountReducer {

	public List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> combine(
			List<KeyValuePair<Character, KeyValuePair<Integer, Integer>>> list) {

		List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> groupByPairs = new ArrayList<GroupByPair<Character, KeyValuePair<Integer, Integer>>>();
		if (list != null) {

			// sort
			WordCountComparator<Character, KeyValuePair<Integer, Integer>> comparator = new WordCountComparator<>();
			comparator.sort(list);

			Character prevKey = Character.MIN_VALUE;
			GroupByPair<Character, KeyValuePair<Integer, Integer>> groupPair = new GroupByPair<Character, KeyValuePair<Integer, Integer>>();
			for (KeyValuePair<Character, KeyValuePair<Integer, Integer>> keyVal : list) {

				Character key = keyVal.getKey();
				KeyValuePair<Integer, Integer> val = keyVal.getValue();

				if (prevKey.toString().toLowerCase().equals(key.toString().toLowerCase())) {
					List<KeyValuePair<Integer, Integer>> values = groupPair.getValues();
					List<KeyValuePair<Integer, Integer>> listValues = new ArrayList<>(values);
					listValues.add(val);
					groupPair.setValues(listValues);
				}

				else {

					if (groupPair.getKey() != null)
						groupByPairs.add(groupPair);
					groupPair = new GroupByPair<>();
					groupPair.setKey(key);
					groupPair.setValues(Arrays.asList(val));
				}

				prevKey = key;
			}
			if (groupPair.getKey() != null)
				groupByPairs.add(groupPair);

			return groupByPairs;
		}

		return null;
	}

	public KeyValuePair<Character, Double> reduce(
			GroupByPair<Character, KeyValuePair<Integer, Integer>> groupByPair) {

		KeyValuePair<Character, KeyValuePair<Integer, Integer>> keyVal = new KeyValuePair<>();
		if (groupByPair != null) {
			Character key = groupByPair.getKey();
			int sum = 0;
			int count = 0;
			for (KeyValuePair<Integer, Integer> val : groupByPair.getValues()) {
				sum += val.getKey();
				count += val.getValue();
			}

			return new KeyValuePair<>(key, (double)sum/(double)count);
		}

		return null;
	}

	public List<KeyValuePair<Character, Double>> wordCountReduce(
			List<GroupByPair<Character, KeyValuePair<Integer, Integer>>> pairs) {

		List<KeyValuePair<Character, Double>> reducedList = new ArrayList<>();
		if (pairs != null) {

			for (GroupByPair<Character, KeyValuePair<Integer, Integer>> pair : pairs) {

				reducedList.add(reduce(pair));
			}

			return reducedList;
		}

		return null;
	}

}
