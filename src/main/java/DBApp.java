import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
	HashSet<String> DB;
	public static int capacity;

	public void init() {
		DB = new HashSet<>();
		// creating the 'data' directory
		// if you get an error copy el path mn 3ndk cuz ubuntu flips the slashes -iman
		String path = "src/main/resources/data/";
		File file = new File(path);
		file.mkdir();
//        String pathcsv = "src/main/resources/";
//        File mata = new File(pathcsv); //todo metadata.csv dynamically

		try {
			capacity = getCapacity();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			addtoDB();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private int getCapacity() throws IOException {
		Properties prop = new Properties();
		String fileName = "src\\main\\resources\\DBApp.config";
		FileInputStream is = new FileInputStream(fileName);
		prop.load(is);

		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
	}

	private void addtoDB() throws IOException {
		FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
		BufferedReader br = new BufferedReader(fr);
		br.readLine();
		while (br.ready()) {
			String line = br.readLine();

			String[] metadata = (line).split(", ");

			DB.add(metadata[0]);
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
			updatemetaindex(tableName, columnNames);
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
		FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
		BufferedReader br = new BufferedReader(fr);
			
		for (int i = 0; i < columnNames.length; i++) {
			String currCol = columnNames[i];
			boolean found = false;
			
				while (br.ready()) {
					String line = br.readLine();
					String[] metadata = (line).split(", ");

					if (metadata[0].equals(tableName)) {

						if (currCol.equals(metadata[1])) {
							found = true;
							minmax.put(currCol, new minMax(metadata[6], metadata[5]));
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

    private void updatemetaindex(String tableName, String[] columnNames) {
        try{
            FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
            BufferedReader br = new BufferedReader(fr);
            String s = "";
            while (br.ready()) {
                String line = br.readLine();

                String[] metadata = (line).split(", ");

                if (metadata[0].equals(tableName)) {
                    for (int i = 0; i < columnNames.length; i++) {
                        if(metadata[1].equals(columnNames)&&metadata[4].equals("false")){
                            s+= metadata[0]+", "+metadata[1]+", "+metadata[2]+", "+metadata[3]+", "+"true, "+metadata[5]+", "+metadata[6];
                        }else{
                            s+=line;
                        }s += "\n";
                    }
                }
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
			boolean useIndex = res.contains(pk);
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

					if (colNameValue.containsKey(metadata[1])) {
						if (metadata[4].equals("True"))
							indexed.add(metadata[1]);
						if (metadata[3].equals("True"))
							pk = metadata[1];
						if (GenericCompare(colNameValue.get(metadata[1]), metadata[5]) < 0
								|| GenericCompare(colNameValue.get(metadata[1]), metadata[6]) > 0) {
							throw new DBAppException("Value is too big or too small ");
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
			return (double) ((String) a).compareTo((String) b);
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {
		if (DB.contains(tableName)) {
			Vector res = checkinMeta(tableName, columnNameValue);
			String pk = (String) res.get(0);
			Vector<String> indexed = (Vector<String>) res.get(1);
			boolean useIndex = res.contains(pk);
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
			Boolean useIndex = false;
			if (res.size() > 1)
				useIndex = true;
			Table table = (Table) deserialize(tableName);
			table.delete(pk, columnNameValue, useIndex);
			serialize(tableName, table);

		} else
			throw new DBAppException("Table does not exist in Database");

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

		// validating input
//        for (int i = 0; i < sqlTerms.length; i++)
//            if ((sqlTerms[i].strOperator).equals(">") || (sqlTerms[i].strOperator).equals(">=") ||
//                    (sqlTerms[i].strOperator).equals("<") || (sqlTerms[i].strOperator).equals("<=") ||
//                    (sqlTerms[i].strOperator).equals("=") || (sqlTerms[i].strOperator).equals("!="))
//                throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");

//        for (int i = 0; i < arrayOperators.length; i++)
//            if (!(arrayOperators[i].equals("AND") || arrayOperators[i].equals("OR") || arrayOperators[i].equals("XOR")))
//                throw new DBAppException("Star operator must be one of AND, OR, XOR!");

		Table table = (Table) deserialize(sqlTerms[0].strTableName);
		if (!(DB.contains(table))) // based on the fact that only one table is included in a select statement
			throw new DBAppException("Table does not exist in Database");

		// todo - check if index exists
		// resolving the select statement

		for (int i = 0; i < sqlTerms.length - 1; i++) {
			if (!(colInTable(sqlTerms[i].strTableName, sqlTerms[i].strColumnName, sqlTerms[i].objValue)))
				throw new DBAppException("Invalid input, check column name and the value's data type");
			Iterator i1 = resolveOneStatement(table, sqlTerms[i]);
			Iterator i2 = resolveOneStatement(table, sqlTerms[i + 1]);
			switch (arrayOperators[i]) {
			case ("AND"):
				return ANDing(i1, i2);
			case ("OR"):
				return ORing(i1, i2);
			case ("XOR"):
				return XORing(i1, i2);
			default:
				throw new DBAppException("Star operator must be one of AND, OR, XOR!");
			}
		}
		return null;
	}

	public Iterator ANDing(Iterator i1, Iterator i2) {
		List<Object> l1 = new ArrayList<>();
		i1.forEachRemaining(l1::add);
		List<Object> l2 = new ArrayList<>();
		i2.forEachRemaining(l2::add);
		ListIterator res = null;
		while (!(l1.isEmpty())) {
			if (l2.contains(l1.get(0)))
				res.add(l1.get(0));
			l1.remove(0);
		}
		return res;
	}

	public Iterator ORing(Iterator i1, Iterator i2) {
		ListIterator res = (ListIterator) i1;
		Object curr;
		while (i2.hasNext()) {
			curr = i2.next();
			res.add(curr);
		}
		Iterator res2 = (Iterator) res;
		return res2;
	}

	public Iterator XORing(Iterator i1, Iterator i2) {
		List<Object> l1 = new ArrayList<>();
		i1.forEachRemaining(l1::add);
		List<Object> l2 = new ArrayList<>();
		i2.forEachRemaining(l2::add);
		ListIterator res = null;
		while (!(l1.isEmpty())) {
			if (!(l2.contains(l1.get(0))))
				res.add(l1.get(0));
			l1.remove(0);
		}
		i1.forEachRemaining(l1::add);
		i2.forEachRemaining(l2::add);
		while (!(l2.isEmpty())) {
			if (!(l1.contains(l2.get(0))))
				res.add(l2.get(0));
			l2.remove(0);
		}
		return res;
	}

	public Iterator resolveOneStatement(Table table, SQLTerm term) throws DBAppException {
		Iterator pagesItr = (table.table).iterator();
		Iterator recs = null;
		Page currPage;
		Page.Pair currRec;

		while (pagesItr.hasNext()) {
			currPage = (Page) pagesItr.next();
			recs = (currPage.records).iterator();
			while (recs.hasNext()) {
				// removing records that violate the select statement
				currRec = (Page.Pair) recs.next();
				if (!(checkCond(currRec, term.strColumnName, term.objValue, term.strOperator)))
					recs.remove();
			}
		}
		return recs;
	}

	public boolean checkCond(Page.Pair rec, String col, Object value, String operator) throws DBAppException {
		Object recVal = rec.row.get(col);
		switch (operator) {
		case ">":
			if (GenericCompare(recVal, value) > 0)
				return true;
		case ">=":
			if (GenericCompare(recVal, value) >= 0)
				return true;
		case "<":
			if (GenericCompare(recVal, value) < 0)
				return true;
		case "<=":
			if (GenericCompare(recVal, value) <= 0)
				return true;
		case "=":
			if (GenericCompare(recVal, value) == 0)
				return true;
		case "!=":
			if (GenericCompare(recVal, value) != 0)
				return true;
		default:
			throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");
		}

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

	public static void main(String[] args) throws Exception {
		DBApp dbApp = new DBApp();
		dbApp.init();
//
//        String dataDirPath = "src/main/resources/data";
//        File dataDir = new File(dataDirPath);
//
//        if (!dataDir.isDirectory() || !dataDir.exists()) {
//            throw new Exception("`data` Directory in Resources folder does not exist");
//        }
//        try{
//            for(String tableName : dbApp.DB){
//                Table table = (Table) DBApp.deserialize(tableName);
//                table.createCSV();
//                DBApp.serialize(tableName,table);
//            }}catch (IOException e){
//            e.printStackTrace();
//        }
	}
//    When to use Index:
//    Age = 20 and city = Egypt
//            (any index is okay)
//
//    Age = 20 or city = Egypt
//            (must have all even in separate indices)
//    Age = 20 xor city = Egypt
//            (must have all even in separate indices)
//
//    Indices :
//            1. age / salary
//  2. salary/city
//    When to use Index:
//    Age = 20 and city = Egypt
//            (any index is okay)
//
//    Age = 20 or city = Egypt
//            (must have all even in separate indices)
//    Age = 20 xor city = Egypt
//            (must have all even in separate indices)
//
//    Indices :
//            1. age / salary
//  2. salary/city

	public static class minMax {
		Object max;
		Object min;

		public minMax(Object max, Object min) {
			this.max = max;
			this.min = min;
		}
	}
}
