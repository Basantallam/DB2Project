import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
	String tableName;
	Vector<tuple4> table;
	Vector<Index> index;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax)
			throws DBAppException, IOException {
		tableName = strTableName;
		this.table = new Vector<tuple4>();
		this.index = new Vector<Index>();
		table.add(new tuple4(Double.valueOf(0), new Page(Double.valueOf(0)), htblColNameMax.get(strClusteringKeyColumn),
				htblColNameMax.get(strClusteringKeyColumn)));
		DBApp.serialize(tableName + "_" + Double.valueOf(0), table.get(0).page);
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

	public void insert(String pk, Hashtable<String, Object> colNameValue, boolean useIndex) {

		Object insertedPkValue = colNameValue.get(pk);
		int foundIdx=0;
		if(useIndex){
			//todo indexinsert
		}
		else foundIdx = BinarySearch(insertedPkValue);
		double foundPageId =table.get(foundIdx).id;
		Page foundpage = (Page) DBApp.deserialize(tableName + "_" + foundPageId);
		tuple4 foundTuple = table.get(foundIdx);// corresponding lel page
		Page.Pair returned = foundpage.insert(insertedPkValue, colNameValue);

		if (returned == null || returned.pk != insertedPkValue) {
			indicesInsert(colNameValue,foundPageId);
			foundTuple.min = foundpage.records.firstElement().pk;
			foundTuple.max = foundpage.records.lastElement().pk;

		}
		DBApp.serialize(tableName + "_" + foundTuple.id, foundpage);

		if (returned != null) {
			Boolean create = true;
			if (table.size() > foundIdx + 1) {
				int nxtIdx = foundIdx + 1;
				Page nxtPage = (Page) DBApp.deserialize(tableName + "_" + table.get(nxtIdx).id);
				if (!nxtPage.isFull()) {
					create = false;
					nxtPage.insert(returned.pk, returned.row);
					if (returned.pk == insertedPkValue)
					indicesInsert(returned.row, nxtPage.id);
					else indicesUpdate(returned.row, foundPageId,nxtPage.id);
					table.get(nxtIdx).min = returned.pk;
				}
				DBApp.serialize(tableName + "_" + nxtPage.id, nxtPage);
			}
			if (create) {
				double newID = CreateID(foundIdx);
				Page newPage = new Page(newID);
				newPage.insert(returned.pk, returned.row);
				if (returned.pk == insertedPkValue) indicesInsert(returned.row, newID);
				else indicesUpdate(returned.row, foundPageId,newID);
				tuple4 newtuple = new tuple4(newID, newPage, returned.pk, returned.pk);
				table.insertElementAt(newtuple, foundIdx + 1);
				DBApp.serialize(tableName + "_" + newID, newPage);
			}

		}


	}

	private void indicesUpdate(Hashtable<String, Object> row, Double oldId, Double newId) {
		for(Index i:index){
			i.updateAddress(row,oldId,newId);
		}
	}

	private void indicesInsert(Hashtable<String, Object> colNameValue, Double id) {
		for(Index i:index){
			i.insert(colNameValue,id);
		}
	}

	private double CreateID(int prevIdx) {
		double prevId = table.get(prevIdx).id;
		if (table.size() == prevIdx + 1)
			return prevId + 1;
		double nxtId = table.get(prevIdx + 1).id;
		return (prevId + nxtId) / 2.0;

	}

	public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue, boolean useIndex) throws Exception {
		Object pk = parse(clusteringKeyValue);
		int idx=0;
		if(useIndex){
			//todo update using Index
		}
		else idx = BinarySearch(pk);
		double pageId=table.get(idx).id;
		Page p = (Page) DBApp.deserialize(tableName + "_" + table.get(idx).id);

		Vector<Hashtable<String, Object>> updatedrows =p.update(pk, columnNameValue);

		DBApp.serialize(tableName + "_" + pageId, p);
		if(updatedrows!=null){
			updateIndices(updatedrows.get(0),updatedrows.get(1),columnNameValue,pageId);
		}
	}

	private void updateIndices(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow, Hashtable<String, Object> updatedValues, double pageId) {
		for(Index i:index){
			i.update(oldRow,newRow,updatedValues,pageId);
		}
	}

	private void indicesDelete(Vector<Hashtable<String, Object>> deletedRows, double pageId) {
		for (Hashtable<String,Object> row:deletedRows)
		for(Index i:index){
			i.delete(row,pageId);
		}
	}

	private Object parse(String clusteringKeyValue) throws Exception {
		Object pk = table.get(0).min;
		if (pk instanceof Integer)
			return Integer.parseInt(clusteringKeyValue);
		else if (pk instanceof Double)
			return Double.parseDouble(clusteringKeyValue);
		else if (pk instanceof Date) {
			return new SimpleDateFormat("yyyy-MM-dd").parse(clusteringKeyValue);
		}

		return clusteringKeyValue;
	}

	public void delete(String pk, Hashtable<String, Object> columnNameValue, Boolean useIndex) {
		if(useIndex){
			//todo delete using Index
		}
		else
		if (pk.equals(""))
			for (tuple4 t : table) {
				Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
				Vector<Hashtable<String,Object>> deletedrows  = p.delete(null, columnNameValue);
				indicesDelete(deletedrows, p.id);

				if (p.isEmpty()) {
					int idx = table.indexOf(t);
					table.remove(idx);
					new File("src/main/resources/data/" + tableName + "_" + t.id + ".ser").delete();
				} else {
					t.min = p.records.firstElement().pk;
					t.max = p.records.lastElement().pk;
					DBApp.serialize(tableName + "_" + t.id, p);
				}

			}
		else {
			Object pkValue = columnNameValue.get(pk);
			int idx = BinarySearch(pkValue);
			tuple4 t = table.get(idx);
			Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
			Vector<Hashtable<String,Object>> deletedrows  = p.delete(null, columnNameValue);
			indicesDelete(deletedrows, p.id);
			if (p.isEmpty()) {
				table.remove(idx);
				new File("src/main/resources/data/" + tableName + "_" + t.id + ".ser").delete();
			} else {
				t.min = p.records.firstElement().pk;
				t.max = p.records.lastElement().pk;
				DBApp.serialize(tableName + "_" + t.id, p);
			}

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
			if (pk.equals(key))
				s += "True, ";
			else
				s += "False, ";

			s += "False, ";
			s += "" + htblColNameMin.get(key) + ", ";
			s += "" + htblColNameMax.get(key);
			s += "\n";
		}
		fw.write(s);
		fw.close();
		br.close();
	}

	public int BinarySearch(Object searchkey) {
		int hi = table.size() - 1; // idx
		int lo = 0;// idx
		return BinarySearch(searchkey, hi, lo);

	}

	public static int GenericCompare(Object a, Object b) {
		if (a instanceof Integer)
			return ((Integer) a).compareTo((Integer) b);
		else if (a instanceof Double)
			return ((Double) a).compareTo((Double) b);
		else if (a instanceof Date || b instanceof Date) {
			if (a instanceof Date && b instanceof Date)
				return ((Date) a).compareTo((Date) b);
			else {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				String formata = a instanceof Date ? formatter.format(a) : (String) a;
				String formatb = b instanceof Date ? formatter.format(b) : (String) b;
				return (formata).compareTo(formatb);
			}
		} else if (a instanceof String)
			return ((String) a).compareTo((String) b);
		else
			return 0;
	}

//	public int BinarySearch(Object searchkey, int hi, int lo) {
//		int mid = (hi + lo) / 2;
//
//		if (lo >= hi)
//			return mid;
//
//		if (GenericCompare(table.get(mid).max, searchkey) > 0 )
//			return BinarySearch(searchkey, mid, lo);
//		else
//			return BinarySearch(searchkey, hi, mid + 1);
//
//	}
	public int BinarySearch(Object searchkey, int hi, int lo) {
		int mid = (hi + lo + 1) / 2;

		if (lo >= hi)
			return mid;

		if (GenericCompare(table.get(mid).min, searchkey) < 0)
			return BinarySearch(searchkey, hi, mid);
		else
			return BinarySearch(searchkey, mid - 1, lo);

	} // better optimization

	public Boolean createIndex(String[] columnNames, Hashtable<String, DBApp.minMax> ranges)  {
		if(checkifexists(columnNames)==false) return false;


		Index i = new Index(this.tableName,columnNames , ranges ,this.table);
		index.add(i);
		return true;
	}

	private boolean checkifexists(String[] columnNames)  {
		//todo
		return true;
	}

	public static class tuple4 implements Serializable {
		Double id;
		transient Page page;
		Object min;
		Object max;

		public String print(String tableName) {
			Page p = (Page) DBApp.deserialize(tableName + "_" + id);

			return p.toString();
		}

		public tuple4(Double id, Page page, Object min, Object max) {
			this.id = id;
			this.page = page;
			this.max = max;
			this.min = min;
		}

	}

	public void createCSV() throws IOException {
		String path = "src\\main\\resources\\Basant\\" + this.tableName + "Table.csv";
		FileWriter fw = new FileWriter(path);

		for (int idx = 0; idx < table.size(); idx++) {
			tuple4 t = table.get(idx);
			Page p = (Page) DBApp.deserialize(tableName + "_" + t.id + "");

			for (Page.Pair pair : p.records) {
				String str = "";
				Hashtable h = pair.row;
				Set<String> s = h.keySet();
				for (String o : s) {
					str += h.get(o).toString() + ", ";
				}
				str += "\n";

				fw.write(str);
			}
			DBApp.serialize(tableName + "_" + t.id + "", p);
		}
		fw.close();
	}

}
