package cs522.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class WordComparator<K, V> {

	@SuppressWarnings("hiding")
	public <K extends Comparable<? super K>> List<KeyValuePair<K, V>> sort(List<KeyValuePair<K, V>> list) {

		Collections.sort(list, new Comparator<KeyValuePair<K, V>>() {

			@Override
			public int compare(KeyValuePair<K, V> o1, KeyValuePair<K, V> o2) {

				K key1 = o1.getKey();
				K key2 = o2.getKey();
				
				if (key1 instanceof String && key2 instanceof String)
					return key1.toString().toLowerCase().compareTo(key2.toString().toLowerCase());
				return key1.compareTo(key2);
			}
		});

		return list;
	}

}
