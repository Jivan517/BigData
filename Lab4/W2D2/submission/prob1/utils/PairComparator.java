package cs522.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PairComparator<K, V, V1> {
	@SuppressWarnings("hiding")
	
	public <K extends Comparable<? super K>, V extends Comparable<? super V>> List<KeyValuePair<KeyValuePair<K, V>, V1>> sort(List<KeyValuePair<KeyValuePair<K, V>, V1>> list) {

		Collections.sort(list, new Comparator<KeyValuePair<KeyValuePair<K, V>, V1>>() {

			@Override
			public int compare(KeyValuePair<KeyValuePair<K, V>, V1> o1, KeyValuePair<KeyValuePair<K, V>, V1> o2) {

				KeyValuePair<K, V> firstPair = o1.getKey();
				KeyValuePair<K, V> secondPair = o2.getKey();

				if (firstPair.getKey() instanceof String && secondPair.getKey() instanceof String
						&& firstPair.getValue() instanceof String && secondPair.getValue() instanceof String){
					
					int pairComparision = firstPair.getKey().toString().toLowerCase().compareTo(secondPair.getKey().toString().toLowerCase());
					if( pairComparision != 0) return pairComparision;
					
					pairComparision = firstPair.getValue().toString().toLowerCase().compareTo(secondPair.getValue().toString().toLowerCase());
					return pairComparision;
					
				}
				
				int pairComparision = firstPair.getKey().compareTo(secondPair.getKey());
				if( pairComparision != 0) return pairComparision;
				
				pairComparision = firstPair.getValue().compareTo(secondPair.getValue());
				return pairComparision;
			}
		});

		return list;
	}
	
	public <K extends Comparable<? super K>, V extends Comparable<? super V>> List<KeyValuePair<KeyValuePair<K, V>, V1>> sortDescAsc(List<KeyValuePair<KeyValuePair<K, V>, V1>> list) {

		Collections.sort(list, new Comparator<KeyValuePair<KeyValuePair<K, V>, V1>>() {

			@Override
			public int compare(KeyValuePair<KeyValuePair<K, V>, V1> o1, KeyValuePair<KeyValuePair<K, V>, V1> o2) {

				KeyValuePair<K, V> firstPair = o1.getKey();
				KeyValuePair<K, V> secondPair = o2.getKey();

				if (firstPair.getKey() instanceof String && secondPair.getKey() instanceof String
						&& firstPair.getValue() instanceof String && secondPair.getValue() instanceof String){
					
					int pairComparision = secondPair.getKey().toString().toLowerCase().compareTo(firstPair.getKey().toString().toLowerCase());
					if( pairComparision != 0) return pairComparision;
					
					pairComparision = firstPair.getValue().toString().toLowerCase().compareTo(secondPair.getValue().toString().toLowerCase());
					return pairComparision;
					
				}
				
				int pairComparision = secondPair.getKey().compareTo(firstPair.getKey());
				if( pairComparision != 0) return pairComparision;
				
				pairComparision = firstPair.getValue().compareTo(secondPair.getValue());
				return pairComparision;
			}
		});

		return list;
	}

}
