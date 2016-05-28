package cs522.utils;


public class KeyValuePair<K, V> {

	private K key;
	private V value;

	public KeyValuePair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public KeyValuePair() {

	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "(" + key + "," + value + ")";
	}

	@Override
	public boolean equals(Object o) {

		if (o == null)
			return false;
		if (o instanceof KeyValuePair == false)
			return false;

		@SuppressWarnings("unchecked")
		KeyValuePair<K, V> pair = (KeyValuePair<K, V>) o;
		if (pair.key == this.key && pair.value == this.value)
			return true;

		return false;

	}

	@Override
	public int hashCode() {

		return this.key.hashCode() + this.value.hashCode();

	}
}
