package Hybrid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Neighbor implements WritableComparable<Neighbor> {

	private Text _primary;
	private Text _foreign;

	public Neighbor() {
		set(new Text(), new Text());
	}

	public Neighbor(String primary, String foreign) {
		set(new Text(primary), new Text(foreign));
	}

	public Neighbor(Text primary, Text foreign) {
		set(primary, foreign);
	}

	public void set(Text primary, Text foreign) {
		this._primary = primary;
		this._foreign = foreign;
	}

	public Text getPrimary() {
		return _primary;
	}

	public Text getForeign() {
		return _foreign;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		this._primary.write(out);
		this._foreign.write(out);
	}

	@Override
	public int compareTo(Neighbor o) {
		int termComp = this._primary.compareTo(o._primary);
		if (termComp != 0)
			return termComp;
		return this._foreign.compareTo(o._foreign);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		this._primary.readFields(in);
		this._foreign.readFields(in);

	}

	@Override
	public int hashCode() {
		return _primary.hashCode() * 163 + _foreign.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Neighbor) {
			Neighbor tp = (Neighbor) o;
			return _primary.equals(tp._primary) && _foreign.equals(tp._foreign);
		}
		return false;
	}
}
