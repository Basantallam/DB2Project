import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.lang.reflect.*;

public class Table implements Serializable {
	String tableName;
//	String pk;
//	transient Hashtable<String, String> htblColNameType;
//	transient Hashtable<String, String> htblColNameMin;
//	transient Hashtable<String, String> htblColNameMax;
	Vector<tuple4> table; // todo page ranges and ids
	String pktype;

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

		pktype = htblColNameType.get(strClusteringKeyColumn);
		updateMetadata(strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);

		// TODO convert types into the datatypes

	}

	public void insert(String pk, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO
		// if(!(colNameValue.containsKey((Object)this.pk)))

		if (table.isEmpty()) { // will not do binary search and will insert directly
			tuple4 firstpage = new tuple4(0, new Page(0), 0, 0);
			Object firstpk = colNameValue.get(pk);
			firstpage.page.insert(firstpk, colNameValue);

			firstpage.max = firstpk;
			firstpage.min = firstpk;

			table.add(firstpage);
			DBApp.serialize(tableName + "_0", firstpage.page);
		} else {
			Page foundpage = BinarySearch(colNameValue.get(pk)); // todo deserialize and return page

			if (foundpage.isFull()) {
				double foundID = foundpage.id;
				double newID = CreateID(foundID);
				Page newPage = new Page(newID);
				tuple4 newtuple = new tuple4(newID, newPage, pk, pk);

				for (int idx = 0; idx < table.size(); idx++) {
					tuple4 tuple = table.get(idx);
					if (tuple.id == foundID) {
						table.insertElementAt(newtuple, idx + 1);
					
						if (GenericCompare(foundpage.records.lastElement().pk,(Object)pk) <= 0) {

						}
						// lesa we need to know which record will be inserted in new page & we need to
						// call foundpage.insert()
						
						break;
					}
				}
			} else {

				foundpage.insert(colNameValue.get(pk), colNameValue);// TODO
				DBApp.serialize(tableName + "_0", foundpage.id);
			}
		}

		// TODO now we search for correct location then we insert
		// shift records if necessary
		// create a new page if necessary and if you'll create new page create a new
		// range too

	}

	private double CreateID(double foundID) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODo update range
		Page foundpage = BinarySearch(columnNameValue.get(clusteringKeyValue)); // TODO currently returns null
		foundpage.update(clusteringKeyValue, columnNameValue);// TODO
	}

	public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {
		// todo access every page todelete records
		for (tuple4 t : table) {
			t.page.delete(columnNameValue);
			// TODO delete entire page if last record is deleted in table use isEmpty()
			// delete it in range vector too
			if (t.page.isEmpty()) {
				int idx = table.indexOf(t.page);
				table.remove(idx);
				// todo delete its binary file
			}
		}

	}

	public void updateMetadata(String pk, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {

		String path = "src\\main\\resources\\metadata.csv";
		FileWriter fw = new FileWriter(path);

		Set<String> keys = htblColNameType.keySet();
		String s = "";
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
			s += "\"" + htblColNameMin.get(key) + "\", ";
			s += "\"" + htblColNameMax.get(key) + "\", ";
			s += "\n";
		}
		fw.write(s);
		fw.close();
		// TODO new file or add??
	}

	public Page BinarySearch(Object searchkey) {
		int hi = table.size(); // idx
		int lo = 0;// idx

		return BinarySearch(searchkey, hi, lo);

	}

	public static Double GenericCompare(Object a, Object b) {
		if (a instanceof Integer)
			return (double) ((Integer) a).compareTo((Integer) b);
		else if (a instanceof Double)
			return (double) ((Double) a).compareTo((Double) b);
		else if (a instanceof Date)
			return (double) ((Date) a).compareTo((Date) b);
		else
			return (double) ((String) a).compareTo((String) b);
	}

	public Page BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (GenericCompare(table.get(mid).max, searchkey) > 0)
			return BinarySearch(searchkey, hi, mid);

		else if (GenericCompare(table.get(mid).min, searchkey) < 0)
			return BinarySearch(searchkey, mid, lo);

		else
			return table.get(mid).page;
	}

	public static void main(String args[]) throws DBAppException, IOException {
		String strTableName = "Student";

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");

		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", new String("Ahmed Noor"));
		htblColNameMin.put("gpa", "0");

		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();

		htblColNameMax.put("id", "2343432");
		htblColNameMax.put("name", new String("Ahmed Noor"));
		htblColNameMax.put("gpa", "0.95");

		Table t = new Table(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);

	}

	private class tuple4 {
		double id;
		Page page;
		Object min;
		Object max;

		public tuple4(double id, Page page, Object min, Object max) {
			this.id = id;
			this.page = page;
			this.max = max;
			this.min = min;
		}
	}
}
