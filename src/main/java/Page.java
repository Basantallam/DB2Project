import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;

public class Page implements Serializable {
	Double id;
	Vector<Pair> records;

	public String toString() {
		return records.toString() + "";
	}

	public Page(Double id) {

		records = new Vector<>();
		this.id = id;

	}

	public Pair insert(Object pkvalue, Hashtable<String, Object> colNameValue) {
		Pair newPair = new Pair(pkvalue, colNameValue);
		if (this.isFull()) {
			if (Table.GenericCompare(records.lastElement().pk, pkvalue) < 0)
				return newPair;
			else {
				Pair lastpair = records.lastElement();
				records.add(newPair); // full capacity+1
				records.remove(lastpair);
				return lastpair;
			}
		} else {
			records.add(newPair);
			return null;
		}

	}

	public void update(Object clusteringKeyValue, Hashtable<String, Object> columnNameValue) {

		Pair foundRecord = LinearSearch(clusteringKeyValue);
		if (foundRecord != null)
			for (String s : columnNameValue.keySet()) {
				(foundRecord.row).replace(s, columnNameValue.get(s));
			}
	}

	public Pair LinearSearch(Object searchkey) {
		Iterator<Pair> it = records.iterator();
		while (it.hasNext()) {
			Pair p = it.next();
			if (Table.GenericCompare(p.pk, searchkey) == 0)
				return p;
		}
		return null;
	}
	
//TreeSet does internal Binary Search
//	public int BinarySearch(Object searchkey, int hi, int lo) {
//		int mid = (hi + lo + 1) / 2;
//		if (lo + 1 >= hi) {
//			if (Table.GenericCompare(records.get(lo).pk, searchkey) == 0)
//				return lo;
//			else if (Table.GenericCompare(records.get(hi).pk, searchkey) == 0)
//				return hi;
//			else {
//				if (lo + 1 == hi) {
//					return -1;
//				}
//				return BinarySearch(searchkey, lo, hi);
//
//			}
//		}
//
//		if (Table.GenericCompare(records.get(mid).pk, searchkey) > 0)
//			return BinarySearch(searchkey, hi, mid);
//
//		else if (Table.GenericCompare(records.get(mid).pk, searchkey) < 0)
//			return BinarySearch(searchkey, mid, lo);
//
//		else
//			return mid;
//	}

//	public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {
//        // delete the record
//        Iterator itr = records.iterator();
//        while (itr.hasNext()) {
//            Pair currRec = (Pair) itr.next();
//            Boolean toDelete = true;
//            Set<String> keys = columnNameValue.keySet();
//            for (String key : keys)
//                if(!(currRec.row.get(key).equals(columnNameValue.get(key))))
//                    toDelete=false;
//            if(!toDelete)
//                records.remove(currRec);
//
//        }
//    }
	public void delete(Hashtable<String, Object> columnNameValue) {

		ListIterator<Pair> it = 
	            records.listIterator( records.size() );
	 		 		
		while (it.hasPrevious()) {
			Pair r = it.previous();

			boolean and = true;
			for (String s : columnNameValue.keySet()) {
				if (r.row.get(s) == null || (!r.row.get(s).equals(columnNameValue.get(s)))) {
					and = false;
					break;
				}
			}
			if (and)
				it.remove();
		}
	}

	public boolean isEmpty() {
		return records.isEmpty();
	}

	public boolean isFull() {
		return records.size() == DBApp.capacity;
	}

	public static class Pair implements Serializable, Comparable<Pair> {
		Object pk;
		Hashtable<String, Object> row;

		public Pair(Object pk, Hashtable<String, Object> row) {
			this.pk = pk;
			this.row = row;
		}

		public String toString() {
			return row.toString();
		}

		@Override
		public int compareTo(Pair p) {
			return  Table.GenericCompare(this.pk, p.pk);
		}
	}

}
