import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
	Double id;
	Vector<Pair> records;

	public String toString() {
		return records.toString() + "";
	}

	public Page(Double id) {

		records = new Vector<Pair>();
		this.id = id;

	}

	public Pair insert(Object pkvalue, Hashtable<String, Object> colNameValue) {
		Pair newPair = new Pair(pkvalue, colNameValue);

		if (this.isFull()) {
			if (Table.GenericCompare(records.lastElement().pk, pkvalue) < 0)
				return newPair;
			else {
				int i = LinearSearch(pkvalue);
				records.insertElementAt(newPair, i); // full capacity+1
				return records.remove(DBApp.capacity);
			}
		} else {
			int i = LinearSearch(pkvalue);
			records.add(i, newPair);
			return null;
		}

	}

	public void update(Object clusteringKeyValue, Hashtable<String, Object> columnNameValue) {

//		int idx = BinarySearch(clusteringKeyValue, records.size() - 1, 0);
		int idx = LinearSearch(clusteringKeyValue);


		for (String s : columnNameValue.keySet()) {
			(records.get(idx).row).replace(s, columnNameValue.get(s));
		}


	}

	public int LinearSearch(Object searchkey) {
		int i = 0;
		for (i = 0; i < records.size(); i++)
			if (Table.GenericCompare(records.get(i).pk, searchkey) >= 0)
				break;
		return i;
	}

	public int BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo + 1) / 2;
		if (lo + 1 >= hi) {
			if (Table.GenericCompare(records.get(lo).pk, searchkey) == 0)
				return lo;
			else if (Table.GenericCompare(records.get(hi).pk, searchkey) == 0)
				return hi;
			else {
				if (lo + 1 == hi) {
					return -1;
				}
				return BinarySearch(searchkey, lo, hi);

			}
		}

		if (Table.GenericCompare(records.get(mid).pk, searchkey) > 0)
			return BinarySearch(searchkey, hi, mid);

		else if (Table.GenericCompare(records.get(mid).pk, searchkey) < 0)
			return BinarySearch(searchkey, mid, lo);

		else
			return mid;
	}

	public void delete(Hashtable<String, Object> columnNameValue) {
		int i = records.size() - 1;
		while (i >= 0) {
			Pair r = records.get(i);
			boolean and = true;
			for (String s : columnNameValue.keySet()) {
				if (r.row.get(s) == null || (!r.row.get(s).equals(columnNameValue.get(s)))) {
					and = false;
					break;
				}
			}
			if (and)
				records.remove(r);
			i--;
		}

	}

	public boolean isEmpty() {
		return records.isEmpty();
	}

	public boolean isFull() {
		return records.size() == DBApp.capacity;
	}

	public static class Pair implements Serializable {
		Object pk;
		Hashtable<String, Object> row;

		public Pair(Object pk, Hashtable<String, Object> row) {
			this.pk = pk;
			this.row = row;
		}

		public String toString() {
			return row.toString();
		}
	}

}
