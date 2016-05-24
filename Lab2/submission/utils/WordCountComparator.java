package cs522.lab2.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WordCountComparator<K, V> {

	public <K extends Comparable<? super K>> List<KeyValuePair<K, V>> sort(List<KeyValuePair<K, V>> list) {

		Collections.sort(list, new Comparator<KeyValuePair<K, V>>() {

			// each map has single entry
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
