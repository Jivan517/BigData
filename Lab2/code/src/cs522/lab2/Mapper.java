package cs522.lab2;

import cs522.lab2.utils.KeyValuePair;

public class Mapper <K, V>{

	public KeyValuePair<K,V> map(K token, V value){
		
		return new KeyValuePair<K,V>(token, value);
	}
}
