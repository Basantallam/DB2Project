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

	public boolean updateAddress(double oldAddess, double newAddress, Hashtable<String, Object> values) {
	
		for (Record r : records) {
			if (r.values.equals(values) && r.pageid == oldAddess) {
				r.pageid = newAddress;
			
				return true;
				
			}
		}
		return false;
	}

	private class Record {
		Hashtable values;
		double pageid;

		public Record(Hashtable<String, Object> value, double pageid) {
			this.values = value;
			this.pageid = pageid;
		}

	}
}
