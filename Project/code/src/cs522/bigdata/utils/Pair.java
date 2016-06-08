package cs522.bigdata.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	private Text key;
	private Text value;

	public Pair(Text key, Text value) {
		this.key = key;
		this.value = value;
	}

	public Pair() {

		this.key = new Text();
		this.value = new Text();
	}

	public Text getKey() {
		return this.key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public Text getValue() {
		return this.value;
	}

	public void setValue(Text value) {
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
		if (o instanceof Pair == false)
			return false;

		Pair pair = (Pair) o;
		if (pair.key.equals(this.key) && pair.value.equals(this.value))
			return true;

		return false;

	}

	@Override
	public int hashCode() {

		return this.key.hashCode() + this.value.hashCode();

	}

	@Override
	public void readFields(DataInput input) throws IOException {

			key.readFields(input);
			value.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		key.write(output);
		value.write(output);
	}

	@Override
	public int compareTo(Pair o) {
		int compareOutput = 0;
		if(!this.key.toString().toLowerCase().equals(o.key.toString().toLowerCase()))
			compareOutput =  this.key.toString().toLowerCase().compareTo(o.key.toString().toLowerCase());
		
		else
			compareOutput = this.value.toString().toLowerCase().compareTo(o.value.toString().toLowerCase());
		return compareOutput;
	}

		
}

