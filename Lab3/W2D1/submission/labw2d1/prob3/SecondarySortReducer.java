package cs522.labw2d1.prob3;

import java.util.*;

import cs522.utils.GroupByPair;
import cs522.utils.KeyValuePair;
import cs522.utils.PairComparator;

public class SecondarySortReducer {

	public List<GroupByPair<KeyValuePair<String, String>, String>> combine(List<KeyValuePair<KeyValuePair<String, String>, String>> list) {

		List<GroupByPair<KeyValuePair<String, String>, String>> groupByPairs = new ArrayList<>();
		if (list != null) {

			//sort
			PairComparator<String, String, String> comparator = new PairComparator<>();
			comparator.sortDescAsc(list);
			
			KeyValuePair<String, String> prevKeyPair = new KeyValuePair<>();
			GroupByPair<KeyValuePair<String, String>, String> groupPair = new GroupByPair<>();
			for (KeyValuePair<KeyValuePair<String, String>, String> keyVal : list) {

				KeyValuePair<String, String> keyPair = keyVal.getKey();
				String val = keyVal.getValue();

				if (prevKeyPair.equals(keyPair)) {
					List<String> values = groupPair.getValues();
					List<String> listValues = new ArrayList<>(values);
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

	public KeyValuePair<KeyValuePair<String, String>, String> reduce(GroupByPair<KeyValuePair<String, String>, String> groupByPair) {

		if (groupByPair != null) {
			KeyValuePair<String, String> key = groupByPair.getKey();
			String sum = "";
			for (String val : groupByPair.getValues()) {
				sum += val;
			}

			return new KeyValuePair<>(key, sum);
		}

		return null;
	}

	public List<KeyValuePair<KeyValuePair<String, String>, String>> beginReducer(List<GroupByPair<KeyValuePair<String, String>, String>> pairs) {

		List<KeyValuePair<KeyValuePair<String, String>, String>> reducedList = new ArrayList<>();
		if (pairs != null) {

			for (GroupByPair<KeyValuePair<String, String>, String> pair : pairs) {

				reducedList.add(reduce(pair));
			}

			return reducedList;
		}

		return null;
	}

}
