package Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class MultipleKey implements WritableComparable<MultipleKey> {
	private String first;
	private String second;

	public MultipleKey() {
		// TODO Auto-generated constructor stub
		this.first = "";
		this.second = "";
	}

	public MultipleKey(String a, String b) {
		// TODO Auto-generated constructor stub
		this.first = a;
		this.second = b;
	}

	public String getKey() {
		return this.first;
	}

	public String getValue() {
		return this.second;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, first);
		WritableUtils.writeString(out, second);
	}

	public void readFields(DataInput in) throws IOException {
		this.first = WritableUtils.readString(in);
		this.second = WritableUtils.readString(in);
	}

	public int compareTo(MultipleKey pop) {
		if (pop == null)
			return 0;

		int intcnt = first.compareTo(pop.first);
		return intcnt == 0 ? second.compareTo(pop.second) : intcnt;
	}
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	@Override
	public String toString() {
		return "(" + first.toString() + ", " + second.toString() + ")";
	}

}