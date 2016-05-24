package cs522.lab2.utils;

import java.util.List;

public class GroupByPair<K, V> {

	private K key;
	private List<V> values;
	
	public GroupByPair(K key, List<V> values){
		this.key = key;
		this.values = values;
	}
	
	public GroupByPair(){
		
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public List<V> getValues() {
		return values;
	}

	public void setValues(List<V> values) {
		this.values = values;
	}
	
	@Override
	public String toString(){
		return "[" + key + "," + values + "]";
	}
	
	
}
