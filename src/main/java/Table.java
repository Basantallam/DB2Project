import org.hamcrest.core.StringContains;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

public class Table implements Serializable {
	String tableName;
	String pk;
	Hashtable<String, String> htblColNameType;
	Hashtable<String, String> htblColNameMin;
	Hashtable<String, String> htblColNameMax;
	Vector<Page> Pages;
	transient Vector<Pair> range; // every element corresponds to a page

	//testing
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax)
			throws DBAppException, IOException {
		tableName = strTableName;
		this.Pages = new Vector<Page>();
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		pk = strClusteringKeyColumn;
		range = new Vector<Pair>();
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

		updateMetadata();

		// TODO convert types into the datatypes

	}

	public void insert(Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO
		// if(!(colNameValue.containsKey((Object)this.pk)))
		if (colNameValue.get((Object) this.pk) == null)
			throw new DBAppException(); // TODO
		else {
			// Set<String> original = this.htblColNameType.keySet();
			Set<String> input = colNameValue.keySet();
			for (String key : input) {
				if (!(this.htblColNameType.containsKey(key)))

					throw new DBAppException("column name doesn't exist");// TODO + do we break here?

				if (!(this.htblColNameType.get(key).equals((colNameValue.get(key).getClass()).toString())))
					// checking correct data types
					throw new DBAppException("incorrect datatype");// TODO + do we break here?
				if ((this.htblColNameMax.get(key)).compareTo((colNameValue.get(key).getClass()).toString()) < 0)
					throw new DBAppException("value entered is above max");// TODO + do we break here?
				if ((this.htblColNameMin.get(key)).compareTo((colNameValue.get(key).getClass()).toString()) > 0)
					throw new DBAppException("value entered is below min");// TODO + do we break here?

				if (Pages.isEmpty()) { // will not do binary search and will insert directly
					Page firstpage = new Page();
					firstpage.records.add(colNameValue);

					Object firstpk = colNameValue.get(pk);
					firstpage.max = firstpk;
					firstpage.min = firstpk;
					Pages.add(firstpage);

					Pair firstpair = new Pair(firstpk, firstpk);
					range.add(firstpair);
				} else {
					// binary search for page

					Page foundpage = BinarySearch(colNameValue.get(pk));
					foundpage.insert(colNameValue);// TODO
				}

				// now we search for correct location then we insert
				// shift records if necessary
				// create a new page if necessary and if you'll create new page create a new
				// range too
				
				
			}
		}
	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		// update range
		Page foundpage = BinarySearch(columnNameValue.get(pk)); // TODO currently returns null
		foundpage.update(clusteringKeyValue, columnNameValue);// TODO
	}

	public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {// TODO

		Page foundpage = BinarySearch(columnNameValue.get(pk)); // TODO currently returns null
		foundpage.delete(columnNameValue);// TODO

		// delete entire page if last record is deleted in table use isEmpty()
		// delete it in range vector too
		if (foundpage.isEmpty()) {
			int idx = Pages.indexOf(foundpage);
			Pages.remove(idx);
			range.remove(idx);
		}
	}

	public static class Pair {
		Object max;
		Object min;

		public Pair(Object ma, Object mi) {
			max = ma;
			min = mi;
		}

	}

	public void updateMetadata() throws IOException {

		String path = "C:\\Users\\Bassant\\Desktop\\Sem 6\\Database 2\\DB2Project\\DB2Project\\src\\main\\resources\\metadata.csv";
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
	}

	public Page BinarySearch(Object searchkey) {
		// TODO
		int hi = Pages.size(); // idx
		int lo = 0;// idx

		String pktype = htblColNameType.get(pk);

		if (pktype.equals("java.lang.Integer"))
			return BinarySearchInt((Integer) searchkey, hi, lo);

		else if (pktype.equals("java.lang.Double"))
			return BinarySearchDouble((Double) searchkey, hi, lo);

		else if (pktype.equals("java.lang.Date"))
			return BinarySearchDate((Date) searchkey, hi, lo);

		else if (pktype.equals("java.lang.String"))
			return BinarySearchString((String) searchkey, hi, lo);

		return null; // shouldn't ever reach this
	}

	public Page BinarySearchInt(Integer searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (((Integer) range.get(mid).max) < searchkey)
			return BinarySearchInt(searchkey, hi, mid);

		else if (((Integer) range.get(mid).min) > searchkey)
			return BinarySearchInt(searchkey, mid, lo);

		else
			return Pages.get(mid);
	}

	public Page BinarySearchDouble(Double searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (((Double) range.get(mid).max) < searchkey)
			return BinarySearchDouble(searchkey, hi, mid);

		else if (((Double) range.get(mid).min) > searchkey)
			return BinarySearchDouble(searchkey, mid, lo);

		else
			return Pages.get(mid);
	}

	public Page BinarySearchString(String searchkey, int hi, int lo) {

		int mid = (hi + lo) / 2;

		if (((String) range.get(mid).max).compareTo(searchkey) < 0)
			return BinarySearchString(searchkey, hi, mid);

		else if (((String) range.get(mid).min).compareTo(searchkey) > 0)
			return BinarySearchString(searchkey, mid, lo);

		else
			return Pages.get(mid);
	}

	public Page BinarySearchDate(Date searchkey, int hi, int lo) {

		int mid = (hi + lo) / 2;

		if (((Date) range.get(mid).max).compareTo(searchkey) < 0)
			return BinarySearchDate(searchkey, hi, mid);

		else if (((Date) range.get(mid).min).compareTo(searchkey) > 0)
			return BinarySearchDate(searchkey, mid, lo);

		else
			return Pages.get(mid);
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

}
