import java.io.Serializable;

import java.util.Hashtable;
import java.util.Vector;

public class Index implements Serializable {
	int serialID;
    String tableName;
	String[] columnNames;
	Hashtable<String, DBApp.minMax> ranges;
	Object[] grid;


	public Index(String tableName, String[] columnNames, Hashtable<String, DBApp.minMax> ranges, Vector<Table.tuple4> table) {
	    this.tableName=tableName;
		this.columnNames = columnNames;
		int n = columnNames.length;
		this.ranges=ranges; //O(1) to find cell index


		Object[] temp = new Vector[10];//todo of type
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
//		((Object[]) ((Object[]) ((Object[]) temp[0])[0])[0])[0] = Integer.valueOf(100); tested deepClone
		this.grid = temp;
		this.fill(table);
	}

    private void fillRanges(Hashtable<String,DBApp.minMax> ranges) {
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
    //todo
	}

	public static void main(String[] args) {
//		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
//		Index idx = new Index("tablename",stringarr, new Hashtable<>(), this.table);
//
//		System.out.println((Arrays.deepToString(idx.grid)));// 4d
//		System.out.println(Arrays.deepToString((Object[]) idx.grid[0]));// 3d
//		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.grid[0])[0]))); // 2d
//		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.grid[0])[0])))); // 1d
	}

	public void updateAddress(Hashtable<String, Object> row, Double oldId, Double newId) {//todo
	}

	public void insert(Hashtable<String, Object> colNameValue, Double id) { //todo  binary search cell and bucket then overflow
	}

	public void update(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow, Hashtable<String, Object> updatedValues, double pageId) {
	Boolean update = false;
		for(String s:columnNames)
		 	if(updatedValues.containsKey(s)){update=true; break;}
		if(!update)return;
		//update
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
	    transient Bucket bucket;
//	    Object max;
//	    Object min;

        public BucketInfo() {

			this.id = ++serialID;
            this.bucket = new Bucket(id);
//            this.max = null;
//            this.min = null;
        }
    }


}