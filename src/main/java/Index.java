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
	String clusteringCol;
	public Index(String tableName, String[] columnNames, Hashtable<String, DBApp.minMax> ranges,
				 Vector<Table.tuple4> table, String clusteringCol) {
		this.tableName = tableName;
		this.columnNames=new Vector<>();
		for (int i = 0; i < columnNames.length; i++)
			this.columnNames.add(columnNames[i]);

		int n = columnNames.length;
		this.ranges = ranges; // O(1) to find cell index

		Object[] temp = new Vector[10];// todo of type
		Object[] temp1 = new Object[10];
		this.clusteringCol = clusteringCol; //todo
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
		for (Table.tuple4 t : table) {
			double pageId = t.id;
			Page page = (Page) DBApp.deserialize(tableName + "_" + pageId);
			for (Page.Pair r : page.records)
				insert(r.row, pageId);
			DBApp.serialize(tableName + "_" + pageId, page);
		}
	}

	public Vector<Integer> getCellCoordinates(Hashtable<String, Object> values) {
		Vector<Integer> coordinates = new Vector<Integer>();
		Hashtable<String, Object> colValues = arrangeHashtable(values);

		for (int i = 0; i < columnNames.size(); i++) {
			String colName = columnNames.get(i);
			Object min = ranges.get(colName).min;
			Object max = ranges.get(colName).max;

			Object value = colValues.get(colName);
			if (value == null) {
				coordinates.add(0);
			} else {
				int cellWidth=(Table.GenericCompare(max,min)+1)/10; //range el cell kam raqam
				int idx = (Table.GenericCompare(value, min) / (cellWidth)); // O(1)
				if(Table.GenericCompare(value, min) % (cellWidth)>0){
					idx++; //ceil
				}
				coordinates.add(idx);
			}
		}
		return coordinates;
	}


	public Hashtable<String, Object> arrangeHashtable(Hashtable<String, Object> values) {
		//takhod kol el record teraga3 el values eli fel Index columns bass

		Set<String> set = values.keySet();
		int IndexDimension = columnNames.size();
		Hashtable<String, Object> extracted = new Hashtable<String, Object>();

		for (int ptr = 0; ptr < IndexDimension; ptr++) {
			String col = columnNames.get(ptr);
			if (set.contains(col)) {
				extracted.put(col, values.get(col));
			} else {
				extracted.put(col, null);
			}
		}
		return extracted;
	}


	public void updateAddress(Hashtable<String, Object> row, Double oldId, Double newId) {
		Vector<BucketInfo> cell = getCell(row);

		for (BucketInfo bi : cell) {
			//todo binary search
			Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
			Hashtable<String, Object> arrangedHash = arrangeHashtable(row);
			boolean updated = b.updateAddress(oldId, newId, arrangedHash);
			DBApp.serialize(tableName + "_b_" + bi.id, b);
			if (updated)
				break;
		}
	}

	public int BinarySearchCell(Vector<BucketInfo> cell,Object searchkey, int hi, int lo) {
	//binary search within one cell
		int mid = (hi + lo + 1) / 2;

		if (lo >= hi)
			return mid;

		if (Table.GenericCompare(cell.get(mid).min, searchkey) < 0)
			return BinarySearchCell(cell,searchkey, hi, mid);
		else
			return BinarySearchCell(cell,searchkey, mid - 1, lo);

	}
	public void insert(Hashtable<String, Object> colNameValue, Double id) {
		// todo binary search cell and bucket then overflow
		Vector<BucketInfo> cell = getCell(colNameValue);

		int bucketInfoIdx = BinarySearchCell(cell,colNameValue.get(clusteringCol),0,cell.size()-1);
		BucketInfo foundBI = cell.get(bucketInfoIdx);
		Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + foundBI.id);

		Hashtable<String, Object> arrangedHash = arrangeHashtable(colNameValue);
		Bucket.Record returned = b.insert(arrangedHash, id);

		if (returned==null || !(returned.values.get(clusteringCol).equals(colNameValue.get(clusteringCol)))){
			foundBI.max= b.records.lastElement().values.get(clusteringCol);
			foundBI.min= b.records.firstElement().values.get(clusteringCol);
		}
		DBApp.serialize(tableName + "_b_" + foundBI.id, b);
		foundBI.size++;
		if(returned!=null){
			foundBI.size--;
			boolean create = true;
			if (cell.size()-1 > bucketInfoIdx ) {
				int nxtIdx = bucketInfoIdx + 1;
				Bucket nxtBucket = (Bucket) DBApp.deserialize(tableName + "_" + cell.get(nxtIdx).id);
				if (!nxtBucket.isFull()) {
					create = false;
					nxtBucket.insert(returned.values,id);
				}
					cell.get(nxtIdx).min = returned.values.get(clusteringCol);
				}

		if (create) {

			BucketInfo newBI = new BucketInfo();
			newBI.bucket.insert(returned.values, id);
			newBI.size++;
			cell.insertElementAt(newBI, bucketInfoIdx + 1);
			DBApp.serialize(tableName + "_b_" + foundBI.id+1, b);
		}
		}
	}

	private int CreateBucketID() {
		//todo ++ or double
		return 0;
	}

	public Vector<BucketInfo> getCell(Hashtable<String, Object> colNameValue) {
		Vector cellIdx = getCellCoordinates(colNameValue);
		Object cell = grid[(Integer) cellIdx.get(0)];
		for (int i = 1; i < cellIdx.size(); i++) {
			int x = (Integer) cellIdx.get(i);
			Object y = ((Object[]) cell)[x];
			cell = y;
		}
		return (Vector<BucketInfo>) cell;
	}

	public void update(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow,
					   Hashtable<String, Object> updatedValues, double pageId) {
		Boolean update = false;
		for (String s : columnNames)
			if (updatedValues.containsKey(s)) { //todo if ranges different
				update = true;
				break;
			}
		if (!update)
			return;
		delete(oldRow, pageId);
		insert(newRow, pageId);
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

	public Vector<Double> narrowPageRange() {
//		vector containing min and max pages
		//min el awal w ba3den hi
		return null; //todo
	}

	private class BucketInfo implements Serializable {
		long id;
		int size;
		transient Bucket bucket;
	    Object max;
	    Object min;

		public BucketInfo() {
			this.size = 0;
			this.id = ++serialID;
			this.bucket = new Bucket(id,clusteringCol);
            this.max = null;
            this.min = null;
		}
	}

	public static void main(String[] args) {
//		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
//		Index idx = new Index("tablename",stringarr, new Hashtable<>(), this.table);
//		System.out.println((Arrays.deepToString(idx.grid)));// 4d
//		System.out.println(Arrays.deepToString((Object[]) idx.grid[0]));// 3d
//		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.grid[0])[0]))); // 2d
//		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.grid[0])[0])))); // 1d
	}
}