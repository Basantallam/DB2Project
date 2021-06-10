import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
	HashSet<String> DB;
	public static int capacity;
	public static int indexCapacity;
	public static Hashtable<Character, Long> code = new Hashtable();

	public void init() {
		DB = new HashSet<>();

		String path = "src/main/resources/data/";
		File file = new File(path);
		file.mkdir();
		createMeta();
		getCapacity();
		addtoDB();
		fillCodes();
	}

	private void createMeta() {
		String metaFilePath = "src/main/resources/metadata.csv";
		File metaFile = new File(metaFilePath);

		if (!metaFile.exists()) {
			try {
				PrintWriter pw= new PrintWriter(new File(metaFilePath));
				pw.close();
				pw.flush();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	private void fillCodes() {
		long j=1;
		for (char i = '0'  ; i <='9' ; i++) code.put(i,j++);
		for (char i = 'A'  ; i <='Z' ; i++) code.put(i,j++);
		code.put('-',j++);
		for (char i = 'a'  ; i <='z' ; i++) code.put(i,j++);
	}

	private void getCapacity() {
		Properties prop = new Properties();
		String fileName = "src\\main\\resources\\DBApp.config";
		FileInputStream is = null;
		try {
			is = new FileInputStream(fileName);
			prop.load(is);

			capacity= Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
			indexCapacity= Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	private void addtoDB() {
		FileReader fr = null;
		try {
			fr = new FileReader("src\\main\\resources\\metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			br.readLine();
			while (br.ready()) {
				String line = br.readLine();

				String[] metadata = (line).split(", ");

				DB.add(metadata[0]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		if (!DB.contains(tableName)) {
			DB.add(tableName);
			try {
				Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
				serialize(tableName, t);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else
			throw new DBAppException("table already exists");
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		if (DB.contains(tableName)) {
			Hashtable<String, minMax> ranges = ValidateColumns(tableName, columnNames);// columns not repeated and exist
																						// and return range of every
																						// column
			Table table = (Table) deserialize(tableName);
			boolean except = table.createIndex(columnNames, ranges);
			serialize(tableName, table);
			if (!except)
				throw new DBAppException("Index exists in Database");
			updateMetaIndex(tableName, columnNames);
		} else
			throw new DBAppException("Table does not exist in Database");
	}

	private Hashtable<String, minMax> ValidateColumns(String tableName, String[] columnNames)
			throws DBAppException {
		// columns not repeated and exist and return range of every column
		for (int i = 0; i < columnNames.length; i++)
			for (int j = i + 1; j < columnNames.length; j++) 
				if (columnNames[i].equals(columnNames[j]))
					throw new DBAppException("repeated column names");
	
		Hashtable<String, minMax> minmax = new Hashtable<String, minMax>();
		try {

			
		for (int i = 0; i < columnNames.length; i++) {
			FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
			BufferedReader br = new BufferedReader(fr);
			String currCol = columnNames[i];
			boolean found = false;
			
				while (br.ready()) {
					String line = br.readLine();
					String[] metadata = (line).split(", ");

					if (metadata[0].equals(tableName)) {

						if (currCol.equals(metadata[1])) {
							found = true;
							minmax.put(currCol, new minMax(parsed(metadata[6],metadata[2]), parsed(metadata[5],metadata[2])));
						}
					}
				}
			 
			if (!found) 
				throw new DBAppException("column name does not exist");
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	
		return minmax; 
	}

	private Object parsed(String value, String type) throws ParseException {
		switch (type){
			case("java.lang.Integer"):
				return Integer.parseInt(value);
			case( "java.lang.Double"):
				return Double.parseDouble(value);
			case ( "java.util.Date") :
				return new SimpleDateFormat("yyyy-MM-dd").parse(value);
			default:	return Index.parseString(value);

		}
	}

	private void updateMetaIndex(String tableName, String[] columnNames) {
		HashSet<String> columns =  new HashSet<>();
		Collections.addAll(columns, columnNames);
        try{
            FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
            BufferedReader br = new BufferedReader(fr);
            String s = "";
            while (br.ready()) {
                String line = br.readLine();

                String[] metadata = (line).split(", ");

                if (metadata[0].equals(tableName)) {
                	if(columns.contains(metadata[1])&&metadata[4].equals("False"))
						s+= metadata[0]+", "+metadata[1]+", "+metadata[2]+", "+metadata[3]+", "+"True, "+metadata[5]+", "+metadata[6];
					else{
						s+=line;
					}s += "\n";
                }else s+=line +"\n";
            }String path = "src\\main\\resources\\metadata.csv";
            FileWriter fw = new FileWriter(path);
            fw.write(s);
            fw.close();
            br.close();
        }catch(IOException e){

        }
    }

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		if (DB.contains(tableName)) {
			Vector res = checkinMeta(tableName, colNameValue);
			String pk = (String) res.get(0);
			Vector<String> indexed = (Vector<String>) res.get(1);
			// update and insert
			boolean useIndex = indexed.contains(pk);
			if (pk.equals(""))
				throw new DBAppException("Primary Key is Not Found");
			Table table = (Table) deserialize(tableName);
			table.insert(pk, colNameValue, useIndex);
			serialize(tableName, table);

		} else
			throw new DBAppException("Table does not exist in Database");
	}

	private Vector checkinMeta(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		Hashtable test = (Hashtable) colNameValue.clone();
		String pk = "";
		Vector<String> indexed = new Vector();
		try {
			FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
			BufferedReader br = new BufferedReader(fr);

			while (br.ready()) {
				String line = br.readLine();

				String[] metadata = (line).split(", ");

				if (metadata[0].equals(tableName)) {
					if (metadata[4].equals("True"))
						indexed.add(metadata[1]);
					if (colNameValue.containsKey(metadata[1])) {

						if (metadata[3].equals("True"))
							pk = metadata[1];
						if (GenericCompare(colNameValue.get(metadata[1]), metadata[5]) < 0)
							throw new DBAppException("Inserted Value : "+colNameValue.get(metadata[1]) + " is too small , Minimum value is : " +metadata[5]  );
						if(GenericCompare(colNameValue.get(metadata[1]), metadata[6]) > 0) {
							throw new DBAppException("Inserted Value : " + colNameValue.get(metadata[1])+" is too big , Maximum value is : "+metadata[6]);
						}
						String strColType = metadata[2];
						boolean ex = false;
						switch (strColType) {
						case "java.lang.Integer":
							if (!(colNameValue.get(metadata[1]) instanceof Integer))
								ex = true;
							break;
						case "java.lang.String":
							if (!(colNameValue.get(metadata[1]) instanceof String))
								ex = true;
							break;
						case "java.lang.Date":
							if (!(colNameValue.get(metadata[1]) instanceof Date))
								ex = true;
							break;
						case "java.lang.Double":
							if (!(colNameValue.get(metadata[1]) instanceof Double))
								ex = true;
							break;
						}
						if (ex) {
							throw new DBAppException("column types not compatible");
						}
						test.remove(metadata[1]);
					}
				}
			}

			if (!test.isEmpty()) {
				throw new DBAppException("column name is not found");
			}
		} catch (IOException e) {

		}
		Vector res = new Vector();
		res.add(pk);
		res.add(indexed);
		return res;

	}

	public static Double GenericCompare(Object a, Object b) {
		if (a instanceof Integer)
			return (double) ((Integer) a).compareTo(Integer.parseInt((String) b));
		else if (a instanceof Double)
			return (double) ((Double) a).compareTo(Double.parseDouble((String) b));
		else if (a instanceof Date) {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String format = formatter.format(a);
			return (double) (format).compareTo((String) b);
		} else
			if(((String)a).length() != ((String)b).length()) return (double)((String)a).length()-((String)b).length();
			return (double) ((String) a).compareTo((String) b);
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		if (DB.contains(tableName)) {
			Vector res = checkinMeta(tableName, columnNameValue);
			String pk = (String) res.get(0);
			Vector<String> indexed = (Vector<String>) res.get(1);
			boolean useIndex = indexed.contains(pk);
			if (!pk.equals(""))
				throw new DBAppException("Primary Key is passed to be updated");
			Table table = (Table) deserialize(tableName);
			try {
				table.update(clusteringKeyValue, columnNameValue, useIndex);
			} catch (Exception e) {
				serialize(tableName, table);
				throw new DBAppException("Primary Key Is NOt a Valid Type");
			}
			serialize(tableName, table);
		} else
			throw new DBAppException("Table does not exist in Database");

	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		if (DB.contains(tableName)) {
			Vector res = checkinMeta(tableName, columnNameValue);
			String pk = (String) res.get(0);
			Vector<String> indexed = (Vector<String>) res.get(1);
			Boolean useIndex = useIndexAnd(columnNameValue, indexed);

			Table table = (Table) deserialize(tableName);
			table.delete(pk, columnNameValue, useIndex);
			serialize(tableName, table);

		} else
			throw new DBAppException("Table does not exist in Database");

	}

	private Boolean useIndexAnd(Hashtable<String, Object> columnNameValue, Vector<String> indexed) {
		for (String i : columnNameValue.keySet()) {
			if (indexed.contains(i)) return true;
		}
		return false;
	}

	private Boolean useIndexOr(Vector<String> indexThere, Hashtable<String, Object> columnNameValue) {// search Or /Xor
		for (String s : columnNameValue.keySet()) {
			if (!indexThere.contains(s))
				return false;
		}
		return true;
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		if (!(DB.contains(sqlTerms[0]._strTableName)))
			throw new DBAppException("Table does not exist in Database");
		Table table = (Table) deserialize(sqlTerms[0]._strTableName);
		Iterator res;
		if(sqlTerms.length==1) {
			if (!(colInTable(sqlTerms[0]._strTableName, sqlTerms[0]._strColumnName, sqlTerms[0]._objValue)))
				throw new DBAppException("Invalid input, check column name and the value's data type");
			res=table.resolveOneStatement(sqlTerms[0]).iterator();
		}
		else{
			PrecedenceStack stack = new PrecedenceStack(table);
			res=stack.resolve(sqlTerms,arrayOperators).iterator();
		}
		return res;
	}





	public boolean colInTable(String table, String column, Object value) throws DBAppException {
		// check col exists + check value type
		try {
			FileReader fr = new FileReader("src/main/resources/metadata.csv");
			BufferedReader br = new BufferedReader(fr);

			while (br.ready()) {
				String line = br.readLine();
				String[] metadata = (line).split(", ");

				if (metadata[0].equals(table) && metadata[1].equals(column)) {
					String strColType = metadata[2];
					boolean ex = false;
					switch (strColType) {
					case "java.lang.Integer":
						if (!(value instanceof Integer))
							ex = true;
						break;
					case "java.lang.String":
						if (!(value instanceof String))
							ex = true;
						break;
					case "java.lang.Date":
						if (!(value instanceof Date))
							ex = true;
						break;
					case "java.lang.Double":
						if (!(value instanceof Double))
							ex = true;
						break;
					}
					if (ex)
						throw new DBAppException("column types not compatible");
					else
						return true;
				}

			}
		} catch (IOException e) {

		}
		return false;
	}

	public static void serialize(String filename, Object obj) {
		try {
			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + filename + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(obj);
			out.close();
			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	public static Object deserialize(String filename) {
		Object obj;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\" + filename + ".ser");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			obj = in.readObject();
			in.close();
			fileIn.close();
		} catch (Exception i) {
			i.printStackTrace();
			return null;
		}
		return obj;
	}


	public static class minMax implements Serializable  {
		Object max;
		Object min;

		public minMax(Object max, Object min){
			this.max = max;
			this.min = min;
		}
	}
}
