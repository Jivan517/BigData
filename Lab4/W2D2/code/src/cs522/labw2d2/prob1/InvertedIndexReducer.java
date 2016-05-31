package cs522.labw2d2.prob1;

import java.util.*;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.PairComparator;

public class InvertedIndexReducer {

	public List<GroupByPair<KeyValuePair<String, Integer>, Integer>> combine(
			List<KeyValuePair<KeyValuePair<String, Integer>, Integer>> list) {

		List<GroupByPair<KeyValuePair<String, Integer>, Integer>> groupByPairs = new ArrayList<>();
		if (list != null) {

			// sort
			PairComparator<String, Integer, Integer> comparator = new PairComparator<>();
			comparator.sortDescAsc(list);

			KeyValuePair<String, Integer> prevKeyPair = new KeyValuePair<>();
			GroupByPair<KeyValuePair<String, Integer>, Integer> groupPair = new GroupByPair<>();
			for (KeyValuePair<KeyValuePair<String, Integer>, Integer> keyVal : list) {

				KeyValuePair<String, Integer> keyPair = keyVal.getKey();
				int val = keyVal.getValue();

				if (prevKeyPair.equals(keyPair)) {
					List<Integer> values = groupPair.getValues();
					List<Integer> listValues = new ArrayList<>(values);
					listValues.add(val);
					groupPair.setValues(listValues);
				}

				else {

					if (groupPair.getKey() != null)
						groupByPairs.add(groupPair);
					groupPair = new GroupByPair<>();
					groupPair.setKey(keyPair);
					groupPair.setValues(Arrays.asList(val));
				}

				prevKeyPair = keyPair;
			}
			if (groupPair.getKey() != null)
				groupByPairs.add(groupPair);

			return groupByPairs;
		}

		return null;
	}

	public KeyValuePair<String, KeyValuePair<Integer, Integer>> reduce(
			GroupByPair<KeyValuePair<String, Integer>, Integer> groupByPair) {

		if (groupByPair != null) {
			KeyValuePair<String, Integer> key = groupByPair.getKey();
			int sum = 0;
			for (int val : groupByPair.getValues()) {
				sum += val;
			}

			return new KeyValuePair<>(key.getKey(), new KeyValuePair<>(key.getValue(), sum));
		}

		return null;
	}

	public List<KeyValuePair<String, List<KeyValuePair<Integer, Integer>>>> beginReducer(
			List<GroupByPair<KeyValuePair<String, Integer>, Integer>> pairs) {

		List<KeyValuePair<String, List<KeyValuePair<Integer, Integer>>>> reducedList = new ArrayList<>();

		if (pairs != null) {

			String prevKey = "";
			for (GroupByPair<KeyValuePair<String, Integer>, Integer> pair : pairs) {

				KeyValuePair<String, KeyValuePair<Integer, Integer>> reducedPair = reduce(pair);
				if (prevKey.equals(reducedPair.getKey())) {
					
					List<KeyValuePair<Integer, Integer>> itemCollection = new ArrayList<>(reducedList.get(reducedList.size() - 1).getValue());
							
					//itemCollection.addAll(reducedList.get(reducedList.size() - 1).getValue());
					itemCollection.add(reducedPair.getValue());
					reducedList.get(reducedList.size() - 1).setValue(itemCollection);
					
				} else {
					reducedList.add(new KeyValuePair<>(reducedPair.getKey(), Arrays.asList(
							new KeyValuePair<>(reducedPair.getValue().getKey(), reducedPair.getValue().getValue()))));
				}

				prevKey = reducedPair.getKey();
			}

			return reducedList;
		}

		return null;
	}

}
