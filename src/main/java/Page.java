import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
	double id;

	Vector<Pair> records;

	// TODO class pages
	public Page(double id) {

		records = new Vector<Pair>();
		this.id = id;

	}

	public Pair insert(Object pkvalue, Hashtable<String, Object> colNameValue) throws DBAppException {
		if(this.isEmpty()) {
			records.add(new Pair(pkvalue,colNameValue));
		}else {
			//linear search
		}
		// TODO if full then delete from vector last and return it
		// else return null

		// TODO binary search on pk
		return null;
	}

	public void update(Object clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO binary search then change row in the pair

	}

	public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO delete the record

	}

	public boolean isEmpty() {
		return records.isEmpty();
	}

	public boolean isFull() {
		return records.size() == DBApp.capacity;
	}

	public static class Pair {
		Object pk;
		Hashtable<String, Object> row;

		public Pair(Object pk, Hashtable<String, Object> row) {
			this.pk = pk;
			this.row = row;
		}

	}

}
