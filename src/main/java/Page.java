import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Vector;
import java.util.Collections;
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

				int i = LinearSearch(pkvalue);
//						BinarySearch(pkvalue,records.size()-1,0);
				records.insertElementAt(newPair, i); // full capacity+1
				return records.remove(DBApp.capacity);
			}
		} else {
			int i = LinearSearch(pkvalue);
//					BinarySearch(pkvalue,records.size()-1,0);
			records.add(i, newPair);
			return null;
		}

	}

	public void update(Object clusteringKeyValue, Hashtable<String, Object> columnNameValue) {

//		Pair foundRecord = LinearSearch(clusteringKeyValue);
		int idx=Collections.binarySearch(records,new Pair(clusteringKeyValue,null));
		if(idx>-1 && idx < records.size()) {
			Pair foundRecord = records.get(idx);
			if (foundRecord != null)
				for (String s : columnNameValue.keySet()) {
					(foundRecord.row).replace(s, columnNameValue.get(s));
				}
		}
	}
	public int LinearSearch(Object searchkey) {
		int i = 0;
		for (i = 0; i < records.size(); i++)
			if (Table.GenericCompare(records.get(i).pk, searchkey) >= 0)
				break;
		return i;
	}

//	public Pair LinearSearch(Object searchkey) {
//		Iterator<Pair> it = records.iterator();
//		while (it.hasNext()) {
//			Pair p = it.next();
//			if (Table.GenericCompare(p.pk, searchkey) == 0)
//				return p;
//		}
//		return null;
//	}
	
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
//public int BinarySearch(Object searchkey, int hi, int lo) {
//	int mid = (hi + lo + 1) / 2;
//
//	if (lo >= hi)
//		return mid;
//
//	if (Table.GenericCompare(records.get(mid), searchkey) < 0)
//		return BinarySearch(searchkey, hi, mid);
//	else
//		return BinarySearch(searchkey, mid - 1, lo);
//
//}

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
	public void delete(Object pkValue, Hashtable<String, Object> columnNameValue) {
		if(pkValue==null){
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
		else{
			int idx=Collections.binarySearch(records,new Pair(pkValue,null));
			if(idx>-1 && idx < records.size()) {
				boolean and = true;
				for (String s : columnNameValue.keySet()) {
					if (records.get(idx).row.get(s) == null || (!records.get(idx).row.get(s).equals(columnNameValue.get(s)))) {
						and = false;
						break;
					}
				}
				if (and)
					records.remove(idx);
			}
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
