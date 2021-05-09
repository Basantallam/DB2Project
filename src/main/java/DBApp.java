import java.io.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {
    HashSet<String> DB;
    public static int capacity;

    public void init() {
        DB = new HashSet<>();
        //creating the 'data' directory
        //if you get an error copy el path mn 3ndk cuz ubuntu flips the slashes -iman
        String path = "src/main/resources/data/";
        File file = new File(path);
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
            return (double) (format).compareTo((String) b);
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
            String pk = checkinMeta(tableName, columnNameValue);
            Table table = (Table) deserialize(tableName);
            table.delete(pk, columnNameValue);
            serialize(tableName, table);

        } else throw new DBAppException("Table does not exist in Database");

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        //validating input
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

        //todo - check if index exists
        //resolving the select statement

        for (int i = 0; i < sqlTerms.length -1 ; i++) {
            if (!(colInTable(sqlTerms[i].strTableName, sqlTerms[i].strColumnName, sqlTerms[i].objValue)))
                throw new DBAppException("Invalid input, check column name and the value's data type");
            Iterator i1 = resolveOneStatement(table, sqlTerms[i]);
            Iterator i2 = resolveOneStatement(table, sqlTerms[i + 1]);
            switch (arrayOperators[i]){
                case("AND"):
                    return ANDing(i1,i2);
                case("OR"):
                    return ORing(i1,i2);
                case("XOR"):
                    return XORing(i1,i2);
                default:
                    throw new DBAppException("Star operator must be one of AND, OR, XOR!");
            }
        }
        return null;
    }
    public Iterator ANDing(Iterator i1, Iterator i2){
        List<Object> l1 = new ArrayList<>();
        i1.forEachRemaining(l1::add);
        List<Object> l2 = new ArrayList<>();
        i2.forEachRemaining(l2::add);
        ListIterator  res = null;
        while(!(l1.isEmpty())){
            if(l2.contains(l1.get(0)))
                res.add(l1.get(0));
            l1.remove(0);
        }
        return res;
    }
    public Iterator ORing(Iterator i1, Iterator i2){
        ListIterator res = (ListIterator) i1;
        Object curr;
        while(i2.hasNext()){
            curr = i2.next();
            res.add(curr);
        }
        Iterator res2 = (Iterator) res;
        return res2;
    }
    public Iterator XORing(Iterator i1, Iterator i2){
        List<Object> l1 = new ArrayList<>();
        i1.forEachRemaining(l1::add);
        List<Object> l2 = new ArrayList<>();
        i2.forEachRemaining(l2::add);
        ListIterator  res =null;
        while(!(l1.isEmpty())){
            if(!(l2.contains(l1.get(0))))
                res.add(l1.get(0));
            l1.remove(0);
        }
        i1.forEachRemaining(l1::add);
        i2.forEachRemaining(l2::add);
        while(!(l2.isEmpty())){
            if(!(l1.contains(l2.get(0))))
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
                //removing records that violate the select statement
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
        //check col exists + check value type
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
                            if (!(value instanceof Integer)) ex = true;
                            break;
                        case "java.lang.String":
                            if (!(value instanceof String)) ex = true;
                            break;
                        case "java.lang.Date":
                            if (!(value instanceof Date)) ex = true;
                            break;
                        case "java.lang.Double":
                            if (!(value instanceof Double)) ex = true;
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
}
