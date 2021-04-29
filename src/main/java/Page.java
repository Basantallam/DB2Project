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
		Pair newPair = new Pair(pkvalue, colNameValue);
		
		if (this.isEmpty()) {
			records.add(new Pair(pkvalue, colNameValue));
			return null;
			
		} else if (this.isFull()) {
			if (Table.GenericCompare(records.lastElement().pk, pkvalue) < 0) 
				return newPair;
			 else {
				int i = 0;

				for (i = 0; i < DBApp.capacity; i++)
					if (Table.GenericCompare(records.get(i).pk, pkvalue) >= 0)
						break;

				records.insertElementAt(newPair, i);
				
				return records.remove(records.size() - 1);

			}

		} else {
			int i = 0;
			for (i = 0; i < records.size(); i++)
				if (Table.GenericCompare(records.get(i).pk, pkvalue) >= 0)
					break;

			records.insertElementAt(newPair, i);
			return null;
		}

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
