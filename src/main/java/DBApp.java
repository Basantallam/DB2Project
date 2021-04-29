import java.io.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface, Serializable {
    Hashtable<String, Table> DB;

    public void init() {
        //Todo load the tables from datatable
        DB = new Hashtable<>();

        //TODO add signature of metatable
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if (!DB.contains(tableName)) {
            try {
                DB.put(tableName, new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
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
            Table table = DB.get(tableName);
            table.insert(colNameValue);  //TODO


        } else throw new DBAppException("Table does not exist in Database");
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            Table table = DB.get(tableName);
            table.update(clusteringKeyValue, columnNameValue);
        } else throw new DBAppException("Table does not exist in Database");

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)) {
            Table table = DB.get(tableName);
            table.delete(columnNameValue);

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
                table = DB.get(sqlTerms[i].strTableName);
                if (!(table.htblColNameType.containsKey(sqlTerms[i].strColumnName)))
                    throw new DBAppException("Column" + sqlTerms[i].strColumnName + "does not exist in Table: " + sqlTerms[i].strTableName);
                else {
                    column = sqlTerms[i].strColumnName;
                    valueType = (sqlTerms[i].objValue.getClass()).toString();
                    if (!(table.htblColNameType.get(column).equals(valueType)))
                        throw new DBAppException("Value has an incorrect data type");
                    else {
                        //todo

                    }
                }
            }
        }
        Iterator pagesItr = (table.Pages).iterator();
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
    public void serialize (String filename , Object obj){
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
    public Object deserialize(String filename){
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
