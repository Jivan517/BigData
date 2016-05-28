package cs522.lab3d5;

import java.util.*;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.PairComparator;

public class PairApproachReducer {

	public List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>> combine(List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> list) {

		List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>> groupByPairs = new ArrayList<>();
		if (list != null) {

			//sort
			PairComparator<Integer, Integer, Integer> comparator = new PairComparator<>();
			comparator.sort(list);
			
			KeyValuePair<Integer, Integer> prevKeyPair = new KeyValuePair<>();
			GroupByPair<KeyValuePair<Integer, Integer>, Integer> groupPair = new GroupByPair<>();
			for (KeyValuePair<KeyValuePair<Integer, Integer>, Integer> keyVal : list) {

				KeyValuePair<Integer, Integer> keyPair = keyVal.getKey();
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

	public KeyValuePair<KeyValuePair<Integer, Integer>, Integer> reduce(GroupByPair<KeyValuePair<Integer, Integer>, Integer> groupByPair) {

		if (groupByPair != null) {
			KeyValuePair<Integer, Integer> key = groupByPair.getKey();
			int sum = 0;
			for (Integer val : groupByPair.getValues()) {
				sum += val;
			}

			return new KeyValuePair<>(key, sum);
		}

		return null;
	}

	public List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> wordCountReduce(List<GroupByPair<KeyValuePair<Integer, Integer>, Integer>> pairs) {

		List<KeyValuePair<KeyValuePair<Integer, Integer>, Integer>> reducedList = new ArrayList<>();
		if (pairs != null) {

			for (GroupByPair<KeyValuePair<Integer, Integer>, Integer> pair : pairs) {

				reducedList.add(reduce(pair));
			}

			return reducedList;
		}

		return null;
	}

}
