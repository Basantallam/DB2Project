import java.io.Serializable;
import java.util.Arrays;

public class Index implements Serializable {
	String[] columnNames;
	Object[] buckets;

	public Index(String[] columnNames) {
		this.columnNames = columnNames;
		int n = columnNames.length;

		Object[] temp = new Object[10];
		Object[] temp1 = new Object[10];
		for (int i = 0; i < 10; i++) {
			temp[i] = new Object();
		}
		for (int i = 1; i < n; i++) {
			for (int j = 0; j < 10; j++)
				temp1[j] = deepClone(temp);
			temp = temp1;
			temp1 = new Object[10];
		}
//		((Object[]) ((Object[]) ((Object[]) temp[0])[0])[0])[0] = Integer.valueOf(100); tested deepClone
		this.buckets = temp;
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

	private void fill() {

	}

	public static void main(String[] args) {
		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
		Index idx = new Index(stringarr);

		System.out.println((Arrays.deepToString(idx.buckets)));// 4d
		System.out.println(Arrays.deepToString((Object[]) idx.buckets[0]));// 3d
		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.buckets[0])[0]))); // 2d
		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.buckets[0])[0])))); // 1d
	}
}