import java.io.Serializable;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Index implements Serializable {
	int serialID;
	String tableName;
	Vector<String> columnNames;
	Hashtable<String, DBApp.minMax> ranges;
	Object[] grid;

	public Index(String tableName, String[] columnNames, Hashtable<String, DBApp.minMax> ranges,
			Vector<Table.tuple4> table) {
		this.tableName = tableName;
		for (int i = 0; i < columnNames.length; i++)
			this.columnNames.add(columnNames[i]);

		int n = columnNames.length;
		this.ranges = ranges; // O(1) to find cell index

		Object[] temp = new Vector[10];// todo of type
		Object[] temp1 = new Object[10];

		for (int i = 0; i < 10; i++) {
			temp[i] = new Vector<BucketInfo>();
		}
		for (int i = 1; i < n; i++) {
			for (int j = 0; j < 10; j++)
				temp1[j] = deepClone(temp);
			temp = temp1;
			temp1 = new Object[10];
		}

		this.grid = temp;
		this.fill(table);

	}

	private Vector<DBApp.minMax> arrangeRanges(Hashtable<String, DBApp.minMax> ranges) {
		Set<String> set = ranges.keySet();
		int IndexDimension = columnNames.size();
		Vector<DBApp.minMax> arrangedRanges = new Vector<DBApp.minMax>();

		for (int ptr = 0; ptr < IndexDimension; ptr++) {
			String col = columnNames.get(ptr);
			if (set.contains(col))
				arrangedRanges.add(ranges.get(col));
		}
		return arrangedRanges;
	}

	public Object deepClone(Object[] org) {
		Object[] clone = new Object[org.length];
		if (org[0] instanceof Object[]) {
			for (int i = 0; i < org.length; i++)
				clone[i] = deepClone((Object[]) org[i]);
		} else
			clone = org.clone();

		return clone;
	}

	private void fill(Vector<Table.tuple4> table) {
		for(Table.tuple4 t:table){
			double pageId= t.id;
			Page page = (Page) DBApp.deserialize(tableName+"_"+pageId);
			for (Page.Pair r :page.records)
				insert(r.row,pageId);
			DBApp.serialize(tableName+"_"+pageId,page);
		}
	}

	public Vector<Integer> getCell(Hashtable<String, Object> values) {
		Vector<Integer> coordinates = new Vector<Integer>();
		Object[] arrangedValues = arrangeValues(values);

		for (int i = 0; i < columnNames.size(); i++) {
			Object min = ranges.get(i).min;
			Object value = arrangedValues[i];
			int idx = (Table.GenericCompare(value, min) / 10); // O(1)
			coordinates.add(idx);
		}
		return coordinates;
	}

	public Object[] arrangeValues(Hashtable<String, Object> values) {
		Set<String> set = values.keySet();
		int IndexDimension = columnNames.size();
		Object[] extracted = new Object[IndexDimension];

		for (int ptr = 0; ptr < IndexDimension; ptr++) {
			String col = columnNames.get(ptr);
			if (set.contains(col)) {
				extracted[ptr] = values.get(col);
			} else {
				extracted[ptr] = 0;
			}
		}
		return extracted;
	}

	public Hashtable<String, Object> arrangeHashtable(Hashtable<String, Object> values) {
		Set<String> set = values.keySet();
		int IndexDimension = columnNames.size();
		Hashtable<String, Object> extracted = new Hashtable<String, Object>();

		for (int ptr = 0; ptr < IndexDimension; ptr++) {
			String col = columnNames.get(ptr);
			if (set.contains(col))
				extracted.put(col, values.get(col));

		}
		return extracted;
	}

	public static void main(String[] args) {
//		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
//		Index idx = new Index("tablename",stringarr, new Hashtable<>(), this.table);
//		System.out.println((Arrays.deepToString(idx.grid)));// 4d
//		System.out.println(Arrays.deepToString((Object[]) idx.grid[0]));// 3d
//		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.grid[0])[0]))); // 2d
//		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.grid[0])[0])))); // 1d
	}

	public void updateAddress(Hashtable<String, Object> row, Double oldId, Double newId) {// todo
	}

	public void insert(Hashtable<String, Object> colNameValue, Double id) { // todo binary search cell and bucket then
																			// overflow
		Vector cellIdx = getCell(colNameValue);
		Object cell = grid[(Integer) cellIdx.get(0)];
		for (int i = 1; i < cellIdx.size(); i++) {
			int x = (Integer) cellIdx.get(i);
			Object y = ((Object[]) cell)[x];
			cell = y;
		}
		for (BucketInfo bi : (Vector<BucketInfo>) cell) {
			Bucket b;
			if (bi.size < DBApp.indexCapacity) {
				b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
			} else {
				BucketInfo buc = new BucketInfo();
				b = new Bucket(buc.id);
			}
			Hashtable<String, Object> arrangedHash = arrangeHashtable(colNameValue);

			b.insert(arrangedHash, id);
			DBApp.serialize(tableName + "_b_" + bi.id, b);
		}

	}

	public void update(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow,
			Hashtable<String, Object> updatedValues, double pageId) {
		Boolean update = false;
		for (String s : columnNames)
			if (updatedValues.containsKey(s)) {
				update = true;
				break;
			}
		if (!update)
			return;
		// update
	}

	public void delete(Hashtable<String, Object> row, double pageId) {
//		todo extract value from row related to the index
//		binary search the bucket if sorted
//		deserialize bucket
//		Bucket foundBucket  = null;
//		foundBucket.delete(row, pageId);
//		check to delete bucket
//		serialize it

	}

	private class BucketInfo implements Serializable {
		long id;
		int size;
		transient Bucket bucket;
//	    Object max;
//	    Object min;

		public BucketInfo() {
			this.size = 0;
			this.id = ++serialID;
			this.bucket = new Bucket(id);
//            this.max = null;
//            this.min = null;
		}
	}

}