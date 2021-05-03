import java.io.*;
import java.io.IOException;
import java.lang.invoke.MethodType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    HashSet<String> DB;
    public static int capacity;

    public void init() {
        DB = new HashSet<>();
        //creating the 'data' directory
        String path = "src/main/resources/data2/";
        File file = new File(path);
        if (!file.exists())
            file.mkdir();
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
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if (!DB.contains(tableName)) {
            DB.add(tableName);
            try {
                Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
                serialize(tableName, t);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else throw new DBAppException("table already exists");
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        //part2
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (DB.contains(tableName)) {


            String pk = checkinMeta(tableName, colNameValue);
            if (pk.equals(""))
                throw new DBAppException("Primary Key is Not Found");
            Table table = (Table) deserialize(tableName);
            table.insert(pk, colNameValue);
            serialize(tableName, table);


        } else throw new DBAppException("Table does not exist in Database");
    }

    private String checkinMeta(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable test = (Hashtable) colNameValue.clone();
        String pk = "";
        try {
            FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
            BufferedReader br = new BufferedReader(fr);

            while (br.ready()) {
                String line = br.readLine();

                String[] metadata = (line).split(", ");

                if (metadata[0].equals(tableName)) {
                    if (metadata[3].equals("True") && colNameValue.containsKey(metadata[1])) pk = metadata[1];
                    if (colNameValue.containsKey(metadata[1])) {
                        if (GenericCompare(colNameValue.get(metadata[1]), metadata[5]) < 0
                                || GenericCompare(colNameValue.get(metadata[1]), metadata[6]) > 0) {
                            throw new DBAppException("Value is too big or too small ");
                        }
                        String strColType = metadata[2];
                        boolean ex = false;
                        switch (strColType) {
                            case "java.lang.Integer":
                                if (!(colNameValue.get(metadata[1]) instanceof Integer)) ex = true;
                                break;
                            case "java.lang.String":
                                if (!(colNameValue.get(metadata[1]) instanceof String)) ex = true;
                                break;
                            case "java.lang.Date":
                                if (!(colNameValue.get(metadata[1]) instanceof Date)) ex = true;
                                break;
                            case "java.lang.Double":
                                if (!(colNameValue.get(metadata[1]) instanceof Double)) ex = true;
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
        return pk;

    }

    public static Double GenericCompare(Object a, Object b) {
        if (a instanceof Integer)
            return (double) ((Integer) a).compareTo(Integer.parseInt((String) b));
        else if (a instanceof Double)
            return (double) ((Double) a).compareTo(Double.parseDouble((String) b));
        else if (a instanceof Date) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            String format = formatter.format(a);
            return (double) ((String) format).compareTo((String) b);
        } else
            return (double) ((String) a).compareTo((String) b);
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            String pk = checkinMeta(tableName, columnNameValue);
            if (!pk.equals(""))
                throw new DBAppException("Primary Key is passed to be updated");
            Table table = (Table) deserialize(tableName);
            try {
                table.update(clusteringKeyValue, columnNameValue);
            } catch (Exception e) {
                serialize(tableName, table);
                throw new DBAppException("Primary Key Is NOt a Valid Type");
            }
            serialize(tableName, table);
        } else throw new DBAppException("Table does not exist in Database");

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            checkinMeta(tableName, columnNameValue);
            Table table = (Table) deserialize(tableName);
            table.delete(columnNameValue);
            serialize(tableName, table);

        } else throw new DBAppException("Table does not exist in Database");

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        //validating input
        for (int i = 0; i < sqlTerms.length; i++)
            if ((sqlTerms[i].strOperator).equals(">") || (sqlTerms[i].strOperator).equals(">=") ||
                    (sqlTerms[i].strOperator).equals("<") || (sqlTerms[i].strOperator).equals("<=") ||
                    (sqlTerms[i].strOperator).equals("=") || (sqlTerms[i].strOperator).equals("!="))
                throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");

        for (int i = 0; i < arrayOperators.length; i++)
            if (!(arrayOperators[i].equals("AND") || arrayOperators[i].equals("OR") || arrayOperators[i].equals("XOR")))
                throw new DBAppException("Star operator must be one of AND, OR, XOR!");

        //resolving the select statement

        Table table = null;
        String column;
        String valueType;
//        String[] s = new String [sqlTerms.length];

        for (int i = 0; i < sqlTerms.length; i++) {
            if (!(DB.contains(sqlTerms[i].strTableName)))
                throw new DBAppException("Table does not exist in Database");
            else {
                table = (Table) deserialize(sqlTerms[i].strTableName); //TODO hashset instead of hashtable table in serialized files
//                if (!(table.htblColNameType.containsKey(sqlTerms[i].strColumnName)))
//                    throw new DBAppException("Column" + sqlTerms[i].strColumnName + "does not exist in Table: " + sqlTerms[i].strTableName);
//                else {
//                    column = sqlTerms[i].strColumnName;
//                    valueType = (sqlTerms[i].objValue.getClass()).toString();
//                    if (!(table.htblColNameType.get(column).equals(valueType)))
//                        throw new DBAppException("Value has an incorrect data type");
//                    else {
//                        //todo
//
//                    }
//
//                }  //todo checking in meta
            }
        }
        Iterator pagesItr = (table.table).iterator();
        Iterator recs = null;
        Iterator res = null;
        Page currPage;
        Hashtable currRec;
        while (pagesItr.hasNext()) {
            currPage = (Page) pagesItr.next();
            recs = (currPage.records).iterator();
            while (recs.hasNext()) {
                //removing records that violate the select statement
                currRec = (Hashtable) recs.next();

//                if(checkCond(currRec,) )
//                    recs.remove();
                //todo

            }
//            add the remainder of recs to res here
        }

        return res;

    }

    public boolean checkCond(Hashtable rec, SQLTerm[] sqlTerms) {
        //todo - iman

        return false;
    }

    public static void serialize(String filename, Object obj) {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src\\main\\resources\\data\\" + filename + ".ser");
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

    public static void main(String[] args) throws IOException {
//       Date d= new SimpleDateFormat("yyyy-MM-dd").parse(new Date(1995 - 1900, 4 - 1, 1));

//       System.out.println(Integer.parseInt("a1"));
//        System.out.println(d);
    }
}
