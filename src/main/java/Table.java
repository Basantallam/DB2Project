import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Table implements Serializable  {
	String tableName;
//	String pk;
//	transient Hashtable<String, String> htblColNameType;
//	transient Hashtable<String, String> htblColNameMin;
//	transient Hashtable<String, String> htblColNameMax;
	Vector<tuple4> table; //todo page ranges and ids


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

		updateMetadata( strClusteringKeyColumn,  htblColNameType,
				 htblColNameMin,  htblColNameMax);

		//TODO convert types into the datatypes

	}

	public void insert( String pk ,Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO
		// if(!(colNameValue.containsKey((Object)this.pk)))

				if (table.isEmpty()) { // will not do binary search and will insert directly
					tuple4 firstpage = new tuple4(0,new Page(0),0,0);
					Object firstpk = colNameValue.get(pk);
					firstpage.page.insert(firstpk,colNameValue);


					firstpage.max = firstpk;
					firstpage.min = firstpk;

					table.add(firstpage);
					DBApp.serialize(tableName+"_0",firstpage.page);
				} else {
					//TODO binary search for page

					Page foundpage = BinarySearch(colNameValue.get(pk)); //todo deserialize and return page
					foundpage.insert(colNameValue.get(pk),colNameValue);// TODO
					DBApp.serialize(tableName+"_0",foundpage.id);
				}

				//TODO now we search for correct location then we insert
				// shift records if necessary
				// create a new page if necessary and if you'll create new page create a new
				// range too


	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
		//TODo update range
		Page foundpage = BinarySearch(columnNameValue.get(clusteringKeyValue)); // TODO currently returns null
		foundpage.update(clusteringKeyValue, columnNameValue);// TODO
	}

	public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {
		// todo access every page todelete records
		for( tuple4 t  : table){
			 t.page.delete(columnNameValue);
			//TODO delete entire page if last record is deleted in table use isEmpty()
			// delete it in range vector too
			if (t.page.isEmpty()) {
				int idx = table.indexOf(t.page);
				table.remove(idx);
				//todo delete its binary file
			}
		}


	}



	public void updateMetadata(String pk, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws IOException {

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
		//TODO new file or add??
	}

	public Page BinarySearch(Object searchkey) {
		// TODO
		int hi = table.size(); // idx
		int lo = 0;// idx

		if (searchkey instanceof  Integer)return BinarySearchInt((Integer) searchkey, hi, lo);
		else if(searchkey instanceof Double) return BinarySearchDouble((Double) searchkey, hi, lo);
		else  if (searchkey instanceof Date) return BinarySearchDate((Date) searchkey, hi, lo);
		else   return BinarySearchString((String) searchkey, hi, lo);
	}

	public Page BinarySearchInt(Integer searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (((Integer) table.get(mid).max) < searchkey)
			return BinarySearchInt(searchkey, hi, mid);

		else if (((Integer) table.get(mid).min) > searchkey)
			return BinarySearchInt(searchkey, mid, lo);

		else
			return table.get(mid).page;
	}

	public Page BinarySearchDouble(Double searchkey, int hi, int lo) {
		int mid = (hi + lo) / 2;

		if (((Double) table.get(mid).max) < searchkey)
			return BinarySearchDouble(searchkey, hi, mid);

		else if (((Double) table.get(mid).min) > searchkey)
			return BinarySearchDouble(searchkey, mid, lo);

		else
			return table.get(mid).page;
	}

	public Page BinarySearchString(String searchkey, int hi, int lo) {

		int mid = (hi + lo) / 2;

		if (((String) table.get(mid).max).compareTo(searchkey) < 0)
			return BinarySearchString(searchkey, hi, mid);

		else if (((String) table.get(mid).min).compareTo(searchkey) > 0)
			return BinarySearchString(searchkey, mid, lo);

		else
			return table.get(mid).page;
	}

	public Page BinarySearchDate(Date searchkey, int hi, int lo) {

		int mid = (hi + lo) / 2;

		if (((Date) table.get(mid).max).compareTo(searchkey) < 0)
			return BinarySearchDate(searchkey, hi, mid);

		else if (((Date) table.get(mid).min).compareTo(searchkey) > 0)
			return BinarySearchDate(searchkey, mid, lo);

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
