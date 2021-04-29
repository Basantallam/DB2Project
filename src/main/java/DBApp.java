import java.io.*;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class DBApp implements DBAppInterface {
    HashSet<String> DB;
    public static int capacity ;

    public void init() {
        DB = new HashSet<>();
        capacity= getCapacity();
        addtoDB();
        //TODO add signature of metatable
    }

    private int getCapacity() {
        //todo from AppDB.config
        return 250;
    }

    private void addtoDB() {
        //TODO retrieve table names from meta table to DB(this)
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
            //TODO add into metatable
        } else throw new DBAppException(); //TODO
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
            table.insert(colNameValue,pk);
            serialize(tableName,table);


        } else throw new DBAppException("Table does not exist in Database");
    }

    private String checkinMeta(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        //todo retrieve from meta data to chdeck and save pk
//        if (colNameValue.get((Object) pk) == null)
//            throw new DBAppException(); // TODO
//        else {
//            // Set<String> original = this.htblColNameType.keySet();
//            Set<String> input = colNameValue.keySet();
//            // todo all checks will be in AppDB
//            for (String key : input) {
//                if (!(this.htblColNameType.containsKey(key)))
//
//                    throw new DBAppException("column name doesn't exist");// TODO + do we break here?
//
//                if (!(this.htblColNameType.get(key).equals((colNameValue.get(key).getClass()).toString())))
//                    // checking correct data types
//                    throw new DBAppException("incorrect datatype");// TODO + do we break here?
//                if ((this.htblColNameMax.get(key)).compareTo((colNameValue.get(key).getClass()).toString()) < 0)
//                    throw new DBAppException("value entered is above max");// TODO + do we break here?
//                if ((this.htblColNameMin.get(key)).compareTo((colNameValue.get(key).getClass()).toString()) > 0)
//                    throw new DBAppException("value entered is below min");// TODO + do we break here?
//
//            }
//        }

        return null;
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

}
