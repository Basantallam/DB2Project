import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Bucket implements Serializable {
	long id;
	Vector<Record> records;

	public Bucket(long id) {
		this.id = id;
		records = new Vector<Record>();
	}

	public void delete(Hashtable<String, Object> row, double pageId) {

	}

	public void insert(Hashtable<String, Object> colNameValue, Double id) {

	}

	public boolean updateAddress(long oldAddess, long newAddress, Hashtable<String, Object> values) {
		for (Record r : records) {

		}
		return false;

	}

	private class Record implements Comparable<Record> {
		Hashtable values;
		double pageid;

		public Record(Hashtable<String, Object> value, double pageid) {
			this.values = value;
			this.pageid = pageid;
		}

		@Override
		public int compareTo(Record o) {
			Set<String> set = values.keySet();
			for (String col : set) {
				if (o.values.containsKey(col) && 0 == Table.GenericCompare(o.values.get(col), this.values.get(col))) {

				}
			}
			return 0;
		}
	}
}
