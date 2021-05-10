import java.io.Serializable;
import java.util.Arrays;
import java.util.Vector;

public class Index implements Serializable {
    String tableName;
	String[] columnNames;
	Object[] cell;

	public Index(String tableName,String[] columnNames) {
	    this.tableName=tableName;
		this.columnNames = columnNames;
		int n = columnNames.length;

		Object[] temp = new Vector[10];
        Object[] temp1 = new Object[10];
        
		for (int i = 0; i < 10; i++) {
			temp[i] = new Vector<>();
		}
		for (int i = 1; i < n; i++) {
			for (int j = 0; j < 10; j++)
				temp1[j] = deepClone(temp);
			temp = temp1;
			temp1 = new Object[10];
		}
//		((Object[]) ((Object[]) ((Object[]) temp[0])[0])[0])[0] = Integer.valueOf(100); tested deepClone
		this.cell = temp;
		this.fill();
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
//    public Vector[] deepClone(Object[] org) {
//        Vector[] clone = new Vector[org.length];
//        if (org[0] instanceof Vector[]) {
//            for (int i = 0; i < org.length; i++)
//                clone[i] = deepClone((Object[]) org[i]);
//        } else
//            clone = org.clone();
//
//        return clone;
//    }

	private void fill() {

	}

	public static void main(String[] args) {
		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
		Index idx = new Index("tablename",stringarr);

		System.out.println((Arrays.deepToString(idx.cell)));// 4d
		System.out.println(Arrays.deepToString((Object[]) idx.cell[0]));// 3d
		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.cell[0])[0]))); // 2d
		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.cell[0])[0])))); // 1d
	}

    private class BucketInfo {
	    long id;
	    transient Bucket bucket;
	    Object max;
	    Object min;

        public BucketInfo(long id, Bucket bucket, Object max, Object min) {
            this.id = id;
            this.bucket = bucket;
            this.max = max;
            this.min = min;
        }
    }
}