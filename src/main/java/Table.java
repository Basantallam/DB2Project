import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
	String tableName;
	Vector<tuple4> table;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax)
			throws DBAppException, IOException {
		tableName = strTableName;
		this.table = new Vector<tuple4>();
		Set<String> keys = htblColNameType.keySet();
		if (strClusteringKeyColumn.equals("")) {
			throw new DBAppException("please enter a primary key");
		}

		for (String key : keys) {
			if (!(htblColNameType.get(key).equals("java.lang.Integer")
					|| htblColNameType.get(key).equals("java.lang.String")
					|| htblColNameType.get(key).equals("java.lang.Double")
					|| htblColNameType.get(key).equals("java.util.Date")))
				throw new DBAppException("not a valid datatype");

		}

		updateMetadata(strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);


	}

	public void insert(String pk, Hashtable<String, Object> colNameValue)  {

		Object insertedPkValue = colNameValue.get(pk);
		if (table.isEmpty()) {
			tuple4 firstpage = new tuple4(Double.valueOf(0), new Page(Double.valueOf(0)), insertedPkValue, insertedPkValue);
			firstpage.page.insert(insertedPkValue, colNameValue);
			table.add(firstpage);
			DBApp.serialize(tableName + "_" + firstpage.id, firstpage.page);

		} else {
			
			int foundIdx = BinarySearch(insertedPkValue);
			Page foundpage = (Page) DBApp.deserialize(tableName + "_" + table.get(foundIdx).id);
			tuple4 foundTuple = table.get(foundIdx);// corresponding lel page
			Page.Pair returned = foundpage.insert(insertedPkValue, colNameValue);
			if (returned==null || returned.pk !=insertedPkValue){
				foundTuple.min = foundpage.records.get(0).pk;
				foundTuple.max= foundpage.records.lastElement().pk;

			}
			DBApp.serialize(tableName + "_" + foundTuple.id, foundpage);


			if (returned!=null) {
				Boolean create = true;
				if(table.size()>foundIdx+1){
					int nxtIdx= foundIdx+1;
					Page nxtPage = (Page) DBApp.deserialize(tableName + "_" + table.get(nxtIdx).id);
					if (!nxtPage.isFull()){
						create = false;
						nxtPage.insert(returned.pk,returned.row);
						table.get(nxtIdx).min=returned.pk;
					}
					DBApp.serialize(tableName + "_" + nxtPage.id,nxtPage );
				}
				if (create){
					double newID = CreateID(foundIdx); // TODO method
					Page newPage = new Page(newID);
					newPage.insert(returned.pk, returned.row);
					tuple4 newtuple = new tuple4(newID, newPage, returned.pk, returned.pk);
					table.insertElementAt(newtuple, foundIdx + 1); // insertElementAt it shifts w kolo ??
					DBApp.serialize(tableName + "_" + newID, newPage);
				}


			}
		}

	}

	private double CreateID(int prevIdx) {
		double prevId = table.get(prevIdx).id;
		if(table.size()==prevIdx+1)return prevId+1;
		double nxtId = table.get(prevIdx+1).id;
		return (prevId+nxtId)/2.0;

	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		int idx = BinarySearch(columnNameValue.get(clusteringKeyValue));
		Page p= (Page) DBApp.deserialize(tableName + "_" + table.get(idx).id);
		p.update(clusteringKeyValue,columnNameValue);
		DBApp.serialize(tableName + "_" + table.get(idx).id, p);
	}

	public void delete(Hashtable<String, Object> columnNameValue) {
		for (tuple4 t : table) {
			Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
			p.delete(columnNameValue);
			if (p.isEmpty()) {
				int idx = table.indexOf(t);
				table.remove(idx);
				new File("src/main/resources/data/"+tableName+"_"+t.id+".ser").delete();
			}else{
				t.min=p.records.firstElement().pk;
				t.max=p.records.lastElement().pk;
			}DBApp.serialize(tableName + "_" + t.id, p);

		}

	}

	public void updateMetadata(String pk, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {
		FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
		BufferedReader br = new BufferedReader(fr);
		String s = "";
		while (br.ready()) {
			String line = br.readLine();
			s += line;
			s += "\n";
		}

		String path = "src\\main\\resources\\metadata.csv";
		FileWriter fw = new FileWriter(path);

		Set<String> keys = htblColNameType.keySet();

		for (String key : keys) {
			s += tableName + ", ";
			s += key + ", ";
			s += htblColNameType.get(key) + ", ";
			if (pk.equals(key)) {
				s += "True, ";
			} else {
				s += "False, ";
			}
			s += "False, ";
			s += "" + htblColNameMin.get(key) + ", ";
			s += "" + htblColNameMax.get(key);
			s += "\n";
		}
		fw.write(s);
		fw.close();
	}

	public int BinarySearch(Object searchkey) {
		int hi = table.size()-1; // idx
		int lo = 0;// idx

		return BinarySearch(searchkey, hi, lo);

	}

	public static Double GenericCompare(Object a, Object b) {
		if (a instanceof Integer)
			return (double) ((Integer) a).compareTo((Integer) b);
		else if (a instanceof Double)
			return (double) ((Double) a).compareTo((Double) b);
		else if (a instanceof Date || b instanceof Date) {
			if (a instanceof Date && b instanceof Date)
				return (double) ((Date) a).compareTo((Date) b);
			else {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				String formata = a instanceof Date? formatter.format(a):(String) a;
				String formatb = b instanceof Date? formatter.format(b):(String) b;
				return (double) ((String) formata).compareTo((String) formatb);
			}
		} else if (a instanceof String)
			return (double) ((String) a).compareTo((String) b);
		else return null;
	}

	public int BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo + 1) / 2;
		if (lo+1 >= hi) {
			return hi;
		}
		
		if (GenericCompare(table.get(mid).min, searchkey) > 0)
			return BinarySearch(searchkey, hi, mid - 1);

		else if (GenericCompare(table.get(mid).max, searchkey) < 0)
			return BinarySearch(searchkey, mid + 1, lo);

		else
			return mid;
	}

	public static void main(String args[]) {

	}

	public static class tuple4 implements Serializable {
		Double id;
		transient Page page;
		Object min;
		Object max;

		public tuple4(Double id, Page page, Object min, Object max) {
			this.id = id;
			this.page = page;
			this.max = max;
			this.min = min;
		}

	}

}
