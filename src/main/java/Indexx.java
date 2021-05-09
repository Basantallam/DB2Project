import java.io.Serializable;
import java.util.Arrays;

public class Indexx implements Serializable {
	String[] columnNames;
	Object[] buckets;

	public static void main(String[] args) {
		String[] stringarr= {"boo", "bar","foo"}; // n=3
		Indexx idx=new Indexx(stringarr);
		
		System.out.println((Arrays.deepToString(idx.buckets)));//3d
		System.out.println(Arrays.deepToString((Object[]) idx.buckets[0]));//2d
		System.out.println((((Object[]) idx.buckets[0])[0])); //1d
	}
	public Indexx(String[] columnNames) {
		this.columnNames = columnNames;
		int n = columnNames.length;

		Object[] temp = new Object[10];
		Object[] temp1 = new Object[10];
		for(int i=0;i<10;i++) {
			temp[i]=new Object();
		}
		for (int i = 1; i < n; i++) {
			for (int j = 0; j < n; j++) 
				temp1[j] = temp.clone();			
			temp = temp1;
			temp1 = new Object[10];
			
		}
		this.buckets=temp;
//		this.fill();
	}

	private void fill() {
	}

}