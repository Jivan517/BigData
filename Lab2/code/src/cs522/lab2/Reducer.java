package cs522.lab2;

import java.util.List;

import cs522.lab2.utils.GroupByPair;
import cs522.lab2.utils.KeyValuePair;

public class Reducer<K,V> {

	public KeyValuePair<K, V> reduce(GroupByPair<K, V> groupByPair){
		
		List<V> values = groupByPair.getValues();
		
		
		return null;
	}
}
