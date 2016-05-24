package cs522.lab1;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WordCountUtil<K, V>  {

	public <K extends Comparable<? super K>>  List<Map<K, V>> sort(List<Map<K, V>> list) {

		Collections.sort(list, new Comparator<Map<K, V>>() {

			//each map has single entry
			@Override
			public int compare(Map<K, V> o1, Map<K, V> o2) {

				Iterator<K> o1Itr = o1.keySet().iterator();
				K key1 = null;
				while (o1Itr.hasNext()) {
					key1 = o1Itr.next();
				}

				Iterator<K> o2Itr = o2.keySet().iterator();
				K key2 = null;
				while (o2Itr.hasNext()) {
					key2 = o2Itr.next();
				}

				if(key1 instanceof String && key2 instanceof String)
					return key1.toString().toLowerCase().compareTo(key2.toString().toLowerCase());
				return key1.compareTo(key2);
			}
		});

		return list;
	}

	
}
