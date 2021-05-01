import java.io.*;
import java.io.IOException;
import java.lang.invoke.MethodType;
import java.util.*;

public class DBApp implements DBAppInterface {
    HashSet<String> DB;
    public static int capacity ;

    public void init() {
        DB = new HashSet<>();
        try {
            capacity = getCapacity();
        }catch(Exception e){
            e.printStackTrace();
        }try {
            addtoDB();
        }catch(Exception e){
            e.printStackTrace();
        }
        //TODO add signature of metatable
    }

    private int getCapacity() throws IOException {
        Properties prop = new Properties();
        String fileName = "src\\main\\resources\\DBApp.config";
        FileInputStream is = new FileInputStream(fileName);
        prop.load(is);

        //    System.out.println(prop.getProperty("..\\DB2Project-main\\target\\classes\\DBApp.config"));
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
            //    System.out.println(metadata[0]);
        }
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if (!DB.contains(tableName)) {
            DB.add(tableName);
            try {
                Table t = new Table( tableName,  clusteringKey,  colNameType, colNameMin, colNameMax);
                serialize(tableName,t);
            } catch (IOException e) {
                e.printStackTrace();
            }
            // add into metatable is done in the constructor of table
        } else throw new DBAppException("table already exists");
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        //part2
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        if (DB.contains(tableName)) {


            String pk= checkinMeta( tableName,  colNameValue);
            Table table= (Table) deserialize(tableName);
            table.insert(pk ,colNameValue);
            serialize(tableName,table);


        } else throw new DBAppException("Table does not exist in Database");
    }

    private String checkinMeta(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable test = colNameValue;
        String pk="";
        try {
            FileReader fr = new FileReader("src\\main\\resources\\metadata.csv");
            BufferedReader br = new BufferedReader(fr);

            while (br.ready()) {
                String line = br.readLine();

                String[] metadata = (line).split(", ");

                if(metadata[0].equals(tableName)){
                    if(metadata[3].equals("True")&& colNameValue.containsKey(metadata[1]))pk=metadata[1];
                    if(colNameValue.containsKey(metadata[1])){
                        if(Table.GenericCompare(colNameValue.get(metadata[1]),metadata[5])<0
                                ||Table.GenericCompare(colNameValue.get(metadata[1]),metadata[6])>0){
                            throw new DBAppException("Value is too big or too small ");
                        }
                    test.remove(metadata[1]);
                    }
                }
            }
            if(pk.equals(""))
                throw new DBAppException("Primary Key Not Found");
            if(!test.isEmpty()){
                throw new DBAppException("column name not found");
            }
        }catch(IOException e){

        }return pk;

    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            Table table = (Table)deserialize(tableName);
            checkinMeta(tableName,columnNameValue);
            table.update(clusteringKeyValue, columnNameValue);
            serialize(tableName,table);
        } else throw new DBAppException("Table does not exist in Database");

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            Table table = (Table)deserialize(tableName);
            table.delete(columnNameValue);
            serialize(tableName,table);

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

        Table table=null;
        String column;
        String valueType;
//        String[] s = new String [sqlTerms.length];

        for (int i = 0; i < sqlTerms.length; i++) {
            if (!(DB.contains(sqlTerms[i].strTableName)))
                throw new DBAppException("Table does not exist in Database");
            else {
                table = (Table)deserialize(sqlTerms[i].strTableName); //TODO hashset instead of hashtable table in serialized files
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
        while(pagesItr.hasNext()){
            currPage = (Page)pagesItr.next();
            recs = (currPage.records).iterator();
            while(recs.hasNext()){
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
    public boolean checkCond(Hashtable rec, SQLTerm[] sqlTerms){
        //todo - iman

        return false;
    }

    public static void serialize (String filename , Object obj){
        try {
            FileOutputStream fileOut =
                    new FileOutputStream("src\\main\\resources\\data\\"+filename+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();

        } catch (IOException i) {
            i.printStackTrace();
        }
    }
    public static Object deserialize(String filename){
        Object obj;
        try {
            FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\"+filename+".ser");
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
        //System.out.println(getCapacity());
        //init();
    }
}
