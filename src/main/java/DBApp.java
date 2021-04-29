import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface, Serializable {
   transient Hashtable<String,Table> DB;

    public void init() {
        DB = new Hashtable<>();
        //TODO add signature of metatable
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        if(!DB.contains(tableName)){
            DB.put(tableName,new Table(tableName,clusteringKey,colNameType,colNameMin,colNameMax));
            //TODO add into metatable
        }
        else throw new DBAppException(); //TODO
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        //part2
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
       if (DB.contains(tableName)){
      Table table=  DB.get(tableName);
      table.insert(colNameValue);  //TODO


            }
       else throw new DBAppException(); //TODO
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)){
        Table table = DB.get(tableName);
        table.update(clusteringKeyValue, columnNameValue);
         }
        else throw new DBAppException(); //TODO

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if (DB.contains(tableName)){
        Table table = DB.get(tableName);
        table.delete(columnNameValue);

        }
        else throw new DBAppException(); //TODO

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {//TODO
        return null;
    }
}
