import java.io.Serializable;
import java.lang.reflect.GenericArrayType;
import java.util.*;

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

				int i = BinarySearch(pkvalue,records.size()-1,0); // working ???
				records.insertElementAt(newPair, i); // full capacity+1
				return records.remove(DBApp.capacity);
			}
		} else {
			int i = BinarySearch(pkvalue,records.size()-1,0);
			records.add(i, newPair);
			return null;
		}

	}
	public Vector<Hashtable<String, Object>> update(Object clusteringKeyValue, Hashtable<String, Object> columnNameValue) {

		Vector<Hashtable<String, Object>> updatedRows  ;
//		Pair foundRecord = LinearSearch(clusteringKeyValue);
		int idx=Collections.binarySearch(records,new Pair(clusteringKeyValue,null));
		if(idx>-1 && idx < records.size()) {
			Pair foundRecord = records.get(idx);
			if (foundRecord != null){
				updatedRows = new Vector<>();
				updatedRows.add((Hashtable<String, Object>) foundRecord.row.clone());
				for (String s : columnNameValue.keySet()) {
					(foundRecord.row).replace(s, columnNameValue.get(s));
				}
				updatedRows.add(foundRecord.row);
			return updatedRows;
			}
		}
		return null;
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
	public int BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (lo >= hi)
			return mid;

		if (Table.GenericCompare(records.get(mid).pk, searchkey) > 0 )
			return BinarySearch(searchkey, mid, lo);
		else
			return BinarySearch(searchkey, hi, mid + 1);

	}
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
//
	public Vector<Hashtable<String, Object>> delete(Object pkValue, Hashtable<String, Object> columnNameValue) {

		Vector<Hashtable<String, Object>> deletedRows = new Vector<>();
		if(pkValue==null){
			ListIterator<Pair> it = records.listIterator( records.size() );
			while (it.hasPrevious()) {
				Pair r = it.previous();

				boolean and = true;
				for (String s : columnNameValue.keySet()) {
					if (r.row.get(s) == null || (!r.row.get(s).equals(columnNameValue.get(s)))) {
						and = false;
						break;
					}
				}
				if (and){
					deletedRows.add(r.row);
					it.remove();
				}
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
				if (and){
					deletedRows.add(records.get(idx).row);
					records.remove(idx);

				}
			}
		}
		return deletedRows;
	}


	public static boolean checkCond(Hashtable rec, SQLTerm term) throws DBAppException {
		String col= term._strColumnName; Object value=term._objValue; String operator=term._strOperator;
		Object recVal = rec.get(col);
		switch (operator) {
			case (">"): return (Table.GenericCompare(recVal, value) > 0);
			case (">="):return (Table.GenericCompare(recVal, value) >= 0);
			case ("<"): return (Table.GenericCompare(recVal, value) < 0);
			case ("<="):return (Table.GenericCompare(recVal, value) <= 0);
			case ("="): return (Table.GenericCompare(recVal, value) == 0);
			case ("!="):return (Table.GenericCompare(recVal, value) != 0);
			default:throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");
		}
	}
	public boolean isEmpty() {
		return records.isEmpty();
	}
	public boolean isFull() {
		return records.size() == DBApp.capacity;
	}

	public Vector<Hashtable> select(SQLTerm term1, SQLTerm term2) throws DBAppException {
		Vector<Hashtable> res= new Vector<>();
		for (Pair currRec :records) { // adding records that match the select statement
			if (checkCond(currRec.row, term1)&&checkCond(currRec.row,term2))
				res.add(currRec.row);
		}
		return res;
	}
	public Vector<Hashtable> select(SQLTerm term1) throws DBAppException {
		Vector<Hashtable> res= new Vector<Hashtable>();
		for (Pair currRec :records) { // adding records that match the select statement
			if (checkCond(currRec.row, term1))
				res.add(currRec.row);
		}
		return res;
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

		public boolean isEqual(Bucket.Record indexRec) {
			Set<String> cols = indexRec.values.keySet();
			for(String col:cols){
				if(Table.GenericCompare(indexRec.values.get(col),row.get(col))!=0)
				return false;
			}
			return true;
		}
	}

}
