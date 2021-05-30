import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Bucket implements Serializable {
	long id;
	Vector<Record> records;
	String clusteringCol;

	public Bucket(long id,String clust) {
		this.id = id;
		records = new Vector<Record>();
		clusteringCol=clust;
	}

	public void delete(Hashtable<String, Object> row, double pageId) {

	}

	public Record insert(Hashtable<String, Object> colNameValue, Double pageID) {
        Record newRecord = new Record(colNameValue,pageID);
        //todo binary search bucket should be sorted mesh ba insert w khalas
		//  records.add(newRecord);
		Object pkvalue= colNameValue.get(clusteringCol);
		if (this.isFull()) {
			if (Table.GenericCompare(records.lastElement().values.get(clusteringCol), pkvalue) < 0)
				return newRecord;
			else {
				int i = BinarySearch(pkvalue,records.size()-1,0); // if it is working
				records.insertElementAt(newRecord, i); // full capacity+1
				return records.remove(DBApp.capacity);
			}
		} else {
			int i = BinarySearch(pkvalue,records.size()-1,0);
			records.add(i, newRecord);
			return null;
		}

	}
	public int BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (lo >= hi)
			return mid;

		if (Table.GenericCompare(records.get(mid).values.get(clusteringCol), searchkey) > 0 )
			return BinarySearch(searchkey, mid, lo);
		else
			return BinarySearch(searchkey, hi, mid + 1);

	}
	public boolean updateAddress(double oldAddress, double newAddress, Hashtable<String, Object> values) {
		//todo binary search

		for (Record r : records) {
			if (r.values.equals(values) && r.pageid == oldAddress) {
				r.pageid = newAddress;
				return true;
				
			}
		}
		return false;
	}
	public boolean isFull(){
		return this.records.size()==DBApp.indexCapacity;
	}

	class Record {
		Hashtable values;
		double pageid;

		public Record(Hashtable<String, Object> value, double pageid) {
			this.values = value;
			this.pageid = pageid;
		}

	}
}
