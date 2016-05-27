package prob4;

import java.util.*;
import java.util.Map.Entry;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.WordComparator;

public class StripeApproachReducer {

	public List<GroupByPair<Integer, Map<Integer, Integer>>> combine(
			List<KeyValuePair<Integer, Map<Integer, Integer>>> list) {

		List<GroupByPair<Integer, Map<Integer, Integer>>> groupByPairs = new ArrayList<>();
		if (list != null) {

			// sort
			WordComparator<Integer, Map<Integer, Integer>> comparator = new WordComparator<>();
			comparator.sort(list);

			int prevKey = Integer.MIN_VALUE;
			GroupByPair<Integer, Map<Integer, Integer>> groupPair = new GroupByPair<>();
			for (KeyValuePair<Integer, Map<Integer, Integer>> keyVal : list) {

				int key = keyVal.getKey();
				Map<Integer, Integer> val = keyVal.getValue();

				if (prevKey == key) {
					List<Map<Integer, Integer>> values = groupPair.getValues();
					List<Map<Integer, Integer>> listValues = new ArrayList<>(values);
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

	public KeyValuePair<Integer, Map<Integer, Integer>> reduce(
			GroupByPair<Integer, Map<Integer, Integer>> groupByPair) {

		if (groupByPair != null) {
			int key = groupByPair.getKey();
			Map<Integer, Integer> hf = new HashMap<>();

			for (Map<Integer, Integer> val : groupByPair.getValues()) {
				if (hf.isEmpty())
					hf.putAll(val);
				else {
					for (Entry<Integer, Integer> item : val.entrySet()) {
						hf.put(item.getKey(),
								hf.get(item.getKey()) == null ? item.getValue() : hf.get(item.getKey()) + item.getValue());
					}
				}
			}

			return new KeyValuePair<>(key, hf);
		}

		return null;
	}

	public List<KeyValuePair<Integer, Map<Integer, Integer>>> wordCountReduce(
			List<GroupByPair<Integer, Map<Integer, Integer>>> pairs) {

		List<KeyValuePair<Integer, Map<Integer, Integer>>> reducedList = new ArrayList<>();
		if (pairs != null) {

			for (GroupByPair<Integer, Map<Integer, Integer>> pair : pairs) {

				reducedList.add(reduce(pair));
			}

			return reducedList;
		}

		return null;
	}

}
