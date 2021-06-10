import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
    String tableName;
    Vector<tuple4> table;
    Vector<Index> index;
    String clusteringCol;

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
        clusteringCol = strClusteringKeyColumn;
        for (String key : keys) {
            if (!(htblColNameType.get(key).equals("java.lang.Integer")
                    || htblColNameType.get(key).equals("java.lang.String")
                    || htblColNameType.get(key).equals("java.lang.Double")
                    || htblColNameType.get(key).equals("java.util.Date")))
                throw new DBAppException("not a valid datatype");

        }

        updateMetadata(strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);

    }

    public static int GenericCompare(Object a, Object b) {
        if (a instanceof Integer)
            return ((Integer) a).compareTo((Integer) b);
        else if (a instanceof Long)
            return ((Long) a).compareTo((Long) b);
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

    public void insert(String pk, Hashtable<String, Object> colNameValue, boolean useIndex) {
        Object insertedPkValue = colNameValue.get(pk);
        int foundIdx = 0;
        int hi = table.size() - 1; // idx
        int lo = 0;// idx
        if (useIndex) {
            Index chosenIndex = chooseIndexPK();
            Vector<Double> narrowedDown = chosenIndex.narrowPageRange(colNameValue);
            if (narrowedDown.firstElement() != -1) lo = PageIDtoIdx(narrowedDown.firstElement());
            if (narrowedDown.lastElement() != -1) hi = PageIDtoIdx(narrowedDown.lastElement());
        }
        foundIdx = BinarySearch(insertedPkValue, hi, lo);
        double foundPageId = table.get(foundIdx).id;
        Page foundpage = (Page) DBApp.deserialize(tableName + "_" + foundPageId);
        tuple4 foundTuple = table.get(foundIdx);// corresponding lel page
        Vector flag = foundpage.insert(insertedPkValue, colNameValue);
        if(!(boolean)flag.get(0))return;
        Page.Pair returned = (Page.Pair) flag.get(1);
        if (returned == null || !returned.pk .equals( insertedPkValue)) { //mesh el mafroud !(.equals) badal (!=)
            indicesInsert(colNameValue, foundPageId); //insert fel indices el new record
            foundTuple.min = foundpage.records.firstElement().pk;
            foundTuple.max = foundpage.records.lastElement().pk;
        }
        DBApp.serialize(tableName + "_" + foundTuple.id, foundpage);

        if (returned != null) {
            boolean create = true;
            if (table.size() > foundIdx + 1) {
                int nxtIdx = foundIdx + 1;
                Page nxtPage = (Page) DBApp.deserialize(tableName + "_" + table.get(nxtIdx).id);
                if (!nxtPage.isFull()) {
                    create = false;
                    nxtPage.insert(returned.pk, returned.row);
                    if (returned.pk.equals( insertedPkValue))
                        indicesInsert(returned.row, nxtPage.id);//insert fel indices el new record
                    else {
                        indicesUpdate(returned.row, foundPageId, nxtPage.id);  //insert fel indices bel shifted record
                    }
                    table.get(nxtIdx).min = returned.pk;
                    DBApp.serialize(tableName + "_" + table.get(nxtIdx).id, nxtPage);
                }

            }
            if (create) {
                double newID = CreateID(foundIdx);
                Page newPage = new Page(newID);
                newPage.insert(returned.pk, returned.row);
                if (returned.pk .equals( insertedPkValue)) indicesInsert(returned.row, newID);
                else {
                    indicesUpdate(returned.row, foundPageId, newID);
                }
                tuple4 newtuple = new tuple4(newID, newPage, returned.pk, returned.pk);
                table.insertElementAt(newtuple, foundIdx + 1);
                DBApp.serialize(tableName + "_" + newID, newPage);
            }
        }
    }
    private int PageIDtoIdx(Double targetPageID) {
        return BinarySearchPageID(table.size() - 1, 0, targetPageID);
        // Binary search for the page ID
        // todo test this
    }
    public int BinarySearchPageID(int hi, int lo, Double targetID) {
        int mid = (hi + lo + 1) / 2;
        if (hi <= lo) {
            //add extra condition to check id is correct?
            return mid;
        }
        if (table.get(mid).id <= targetID) {
            return BinarySearchPageID(hi, mid, targetID);
        } else {
            return BinarySearchPageID(mid - 1, lo, targetID);
        }
    }
    public Index chooseIndexPK() {
        Index indexSoFar = index.get(0);
        int min = (int) 1e6;
        for (Index i : index) {
            if (i.clusteringCol.equals(clusteringCol)) {
                int size = i.getSize();
                if (size < min) {
                    indexSoFar = i;
                    min = size;
                }
            }
        }
        return indexSoFar;
    }


    public Index chooseIndex(Vector<String> columnNames) {
        Index indexSoFar = null;
        int size = Integer.MAX_VALUE;
        int max = 0;
        for (Index i : index) {
            int count = 0;
            for (String cn : i.columnNames) {
                if (columnNames.contains(cn)) count++;
            }
            if (count > max) {
                max = count;
                indexSoFar = i;
            }else{
                if(count==max && i.columnNames.size()<size) {
                    max = count;
                    indexSoFar = i;
                }
            }
        }
        return indexSoFar;
    }
    public Vector<Index> chooseIndexOr(Vector<String> columnNames) {

        Vector<Index> res = new Vector<>();
        for (String cn : columnNames) {
            for (Index i : index) {
                if (i.columnNames.contains(cn)) res.add(i);
            }
        }
        return res;
    }
    private void indicesUpdate(Hashtable<String, Object> row, Double oldId, Double newId) {
        for (Index i : index) {
            i.updateAddress(row, oldId, newId);
        }
    }
    private void indicesInsert(Hashtable<String, Object> colNameValue, Double id) {
        for (Index i : index) {
            i.insert(colNameValue, id);
        }
    }
    private double CreateID(int prevIdx) {
        double prevId = table.get(prevIdx).id;
        if (table.size() == prevIdx + 1)
            return prevId + 1;
        double nxtId = table.get(prevIdx + 1).id;
        return (prevId + nxtId) / 2.0;

    }
    public void update(String clusteringKeyValue, Hashtable<String, Object> columnNameValue, boolean useIndex)
            throws Exception
    {
        if (table.size()==0)return;
        Object pk = parse(clusteringKeyValue);
        int idx = 0;
        int hi = table.size() - 1; // idx
        int lo = 0;// idx
        if (useIndex) {
            Hashtable<String,Object> clustering = new Hashtable<>();
            clustering.put(clusteringCol,clusteringKeyValue);
            Index chosenIndex = chooseIndexPK();
            Vector<Double> narrowedDown = chosenIndex.narrowPageRange(clustering);
            if (narrowedDown.firstElement() != -1) lo = PageIDtoIdx(narrowedDown.firstElement());
            if (narrowedDown.lastElement() != -1) hi = PageIDtoIdx(narrowedDown.lastElement());
        }
        idx = BinarySearch(pk, hi, lo);
        double pageId = table.get(idx).id;
        Page p = (Page) DBApp.deserialize(tableName + "_" + pageId);
        Vector<Hashtable<String, Object>> updatedRows = p.update(pk, columnNameValue);
        DBApp.serialize(tableName + "_" + pageId, p);

        if (updatedRows != null) {
            updateIndices(updatedRows.get(0), updatedRows.get(1), columnNameValue, pageId);
        }

    }

    private void updateIndices(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow, Hashtable<String, Object> updatedValues, double pageId) {
        for (Index i : index) {
            i.update(oldRow, newRow, updatedValues, pageId);
        }
    }

    private void indicesDelete(Vector<Hashtable<String, Object>> deletedRows, double pageId) {
        for (Hashtable<String, Object> row : deletedRows)
            for (Index i : index) {
                i.delete(row, pageId);
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
        if(table.size()==0)return;
        if (useIndex) {
            if(!pk.equals("")){
                int lo=0; int hi = table.size()-1;
                Index chosenIndex = chooseIndexPK();
                if(chosenIndex==null) {
                    deleteWithPK(columnNameValue,pk,hi,lo);
                    return;
                }
                Vector<Double> narrowedDown = chosenIndex.narrowPageRange(columnNameValue);
                if (narrowedDown.firstElement() != -1) lo = PageIDtoIdx(narrowedDown.firstElement());
                if (narrowedDown.lastElement() != -1) hi = PageIDtoIdx(narrowedDown.lastElement());
                deleteWithPK(columnNameValue,pk,hi,lo);
            }
            else{
                HashSet<Double> ids=chooseIndex(new Vector<>( columnNameValue.keySet())).delete(columnNameValue);
                for (double id:ids) {
                    Page p = (Page) DBApp.deserialize(tableName + "_" + id);
                    Vector<Hashtable<String, Object>> deletedrows = p.delete(null, columnNameValue);
                    int idx=PageIDtoIdx(id);
                    tuple4 t = table.get(idx);
                    indicesDelete(deletedrows, p.id);
                    deleteRefactorPage(p, idx,t);
                }
            }
        } else deleteWithoutIndex(pk, columnNameValue);
    }

    private void deleteWithoutIndex(String pk, Hashtable<String, Object> columnNameValue) {
        if (pk.equals(""))
            for (tuple4 t : table) {
                Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
                Vector<Hashtable<String, Object>> deletedrows = p.delete(null, columnNameValue);
                indicesDelete(deletedrows, p.id);
                int idx = table.indexOf(t);
                deleteRefactorPage(p,idx,t);
            }
        else {
            int hi = table.size() - 1; // idx
            int lo = 0;// idx
            deleteWithPK(columnNameValue, pk, hi, lo);
        }
    }

    private void deleteWithPK(Hashtable<String, Object> columnNameValue, String pk, int hi, int lo) {
        Object pkValue = columnNameValue.get(pk);
        int idx = BinarySearch(pkValue, hi, lo);
        tuple4 t = table.get(idx);
        Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
        Vector<Hashtable<String, Object>> deletedrows = p.delete(null, columnNameValue);
        indicesDelete(deletedrows, p.id);
        deleteRefactorPage(p,idx,t);
    }

    private void deleteRefactorPage(Page page, int idx, tuple4 t) {
        if (page.isEmpty()) {
            table.remove(idx);
            new File("src/main/resources/data/" + tableName + "_" + t.id + ".ser").delete();
        } else {
            t.min = page.records.firstElement().pk;
            t.max = page.records.lastElement().pk;
            DBApp.serialize(tableName + "_" + t.id, page);
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

    public int BinarySearch(Object searchkey, int hi, int lo) {
        int mid = (hi + lo + 1) / 2;
        if (lo >= hi) return mid;
        if (GenericCompare(table.get(mid).min, searchkey) < 0)
            return BinarySearch(searchkey, hi, mid);
        else
            return BinarySearch(searchkey, mid - 1, lo);
    }
    public Boolean createIndex(String[] columnNames, Hashtable<String, DBApp.minMax> ranges) {
        if (checkExists(columnNames)) return false; // check if index already exists
        Index i = new Index(this.tableName, columnNames, ranges, this.table, this.clusteringCol);
        index.add(i);
        return true;
    }
    private boolean checkExists(String[] columnNames) {
        HashSet<String> columns = new HashSet<>();
        Collections.addAll(columns, columnNames);
        loop:
        for (Index i : index) {
            if (i.columnNames.size() == columns.size()) {
                for (int j = 0; j < i.columnNames.size(); j++)
                    if (!columns.contains(i.columnNames.get(j))) continue loop;
                return true;
            }
        }
        return false;
    }
    public Vector<Hashtable> resolveOneStatement(SQLTerm term) throws DBAppException {
        Vector<String> terms = new Vector<String>(); terms.add(term._strColumnName);
        boolean clustered = this.clusteringCol.equals(term._strColumnName);
        if(clustered)
            return tableTraversal(term);
        //!clustered:
        Vector<String> v = new Vector(); v.add(term._strColumnName);
        Index index = chooseIndex(v);
        if (null == index)
            return LinearScan(term);
        return indexTraversal(term, index); //index!=null

    }
    private Vector<Hashtable> indexTraversal(SQLTerm term, Index index) throws DBAppException {
        switch (term._strOperator) {
            case ("<"): case ("<="): return getTableRecords(index.lessThan(term),term);
            case (">"): case (">="): return getTableRecords(index.greaterThan(term),term);
            case ("="):  return getTableRecords(index.equalSelect(term),term);
            case ("!="): return notEqual(term);//doesn't use index
            default: throw new DBAppException("invalid operation");
        }
    }

    private Vector<Hashtable> notEqual(SQLTerm term) throws DBAppException {
        Vector<Hashtable> result = new Vector<Hashtable>();
        Page currPage;
        for(tuple4 tuple :table){
            currPage = (Page) DBApp.deserialize(tableName + "_" + (tuple.id));
            result.addAll(currPage.select(term));
            DBApp.serialize(tableName + "_" + tuple.id, currPage);
        }
        return result;
    }
    Vector<Hashtable> getTableRecords(HashSet<Double> pageID, SQLTerm term1, SQLTerm term2) throws DBAppException {
        Vector<Hashtable> result = new Vector<Hashtable>();
        for(Double id :pageID){
            Page p = (Page) DBApp.deserialize(tableName + "_" + id);
            result.addAll(p.select(term1,term2));
            DBApp.serialize(tableName + "_" + id,p);
        }
        return result;
    }
    private Vector<Hashtable> getTableRecords(HashSet<Double> pageID,SQLTerm term) throws DBAppException {
        Vector<Hashtable> result = new Vector<>();

        for(Double id :pageID){
            Page p = (Page) DBApp.deserialize(tableName + "_" + id);
            result.addAll(p.select(term));
            DBApp.serialize(tableName + "_" + id,p);
        }
        return result;
    }

    public Vector<Hashtable> Equal(SQLTerm term) throws DBAppException {
        int pIdx=this.BinarySearch(term._objValue,table.size()-1,0);
        Page page = (Page) DBApp.deserialize(tableName + "_" + table.get(pIdx).id);
        int rIdx = page.BinarySearch(term._objValue,page.records.size()-1,0);
        Vector result = new Vector();
        if(checkCond(page.records.get(rIdx).row,term))
            result.add(page.records.get(rIdx).row);
        DBApp.serialize(tableName + "_" + table.get(pIdx),page);
        return result;
    }
    Vector<Hashtable> tableTraversal(SQLTerm term) throws DBAppException {
        //only used when table is sorted on queried column
        switch (term._strOperator) {
            case ("<"): case ("<="):return this.lessThan(term);
            case (">"): case (">="): return this.greaterThan(term);
            case ("="): return this.Equal(term);
            case ("!="): return notEqual(term); //  won't use index kda kda
            default:throw new DBAppException("invalid operation");
        }
    }
    public Vector<Hashtable> lessThan(SQLTerm term) throws DBAppException {
        // traverse table used queried col is clustering column
        int pageIdx = BinarySearch(term._objValue,table.size()-1,0);
        Page page = (Page) DBApp.deserialize(tableName + "_" + table.get(pageIdx).id);

        DBApp.serialize(tableName + "_" + table.get(pageIdx).id,page);
        return loopUntil(pageIdx,term);
    }
    public Vector<Hashtable> greaterThan(SQLTerm term) throws DBAppException {
        // traverse table used queried col is clustering column
        int pageIdx = BinarySearch(term._objValue,table.size()-1,0);
        Page page = (Page) DBApp.deserialize(tableName + "_" + table.get(pageIdx).id);

        DBApp.serialize(tableName + "_" + table.get(pageIdx).id,page);
        return loopFrom(pageIdx,term);
    }
    private Vector<Hashtable> LinearScan(SQLTerm term) throws DBAppException {
        //used when queried col is not clustering col + mafeesh index
        Vector res = new Vector();//loop on entire table.. every single record and check
            for(tuple4 tuple:table) {
               Page currPage = (Page) DBApp.deserialize(tableName + "_" + (tuple.id));
                res.addAll(currPage.select(term));
                DBApp.serialize(tableName + "_" + tuple.id, currPage);
            }
            return res;
    }
    public Vector<Hashtable> loopUntil(int pageIdx, SQLTerm term) throws DBAppException {

        Vector<Hashtable> res = new Vector<Hashtable>();
        for(int pIdx=0;pIdx<=pageIdx;pIdx++){
            Page currPage = (Page) DBApp.deserialize(tableName + "_" + table.get(pIdx).id);
            for(int rIdx=0;rIdx<currPage.records.size();rIdx++){
                Page.Pair record =currPage.records.get(rIdx);
                if(pIdx==pageIdx && !checkCond(record.row,term))
                        break;
                res.add(record.row);
            }
            DBApp.serialize(tableName + "_" + table.get(pIdx),currPage);
        }
        return res;
    }

    public Vector<Hashtable> loopFrom(int pageIdx,SQLTerm term) throws DBAppException {
        Vector<Hashtable> res=new Vector<Hashtable>();

        for(int pIdx=pageIdx;pIdx<table.size();pIdx++){
            Page currPage = (Page) DBApp.deserialize(tableName + "_" + table.get(pIdx).id);
            for(int rIdx=0;rIdx<currPage.records.size();rIdx++){
                Page.Pair record =currPage.records.get(rIdx);
                if((pageIdx==pIdx&&checkCond(record.row,term))||pIdx!=pageIdx)
                    res.add(record.row);
            }
            DBApp.serialize(tableName + "_" + table.get(pIdx),currPage);
        }
        return res;
    }

    Vector<Hashtable> LinearScan(SQLTerm term1, SQLTerm term2) throws DBAppException {
        Vector<Hashtable> res = new Vector<Hashtable>();//loop on entire table.. every single record and check
        for(tuple4 tuple:table) {
            Page currPage = (Page) DBApp.deserialize(tableName + "_" + (tuple.id));
            res.addAll(currPage.select(term1,term2));
            DBApp.serialize(tableName + "_" + tuple.id, currPage);
        }
        return res;
    }
    public static boolean checkCond(Hashtable rec, SQLTerm term) throws DBAppException {
        String col= term._strColumnName; Object value=term._objValue; String operator=term._strOperator;
        Object recVal = rec.get(col);
        switch (operator) {
            case (">"): return (Table.GenericCompare(recVal, value) > 0);
            case (">="):return (Table.GenericCompare(recVal, value) >= 0);
            case ("<"): return (Table.GenericCompare(recVal, value) <0);
            case ("<="):return (Table.GenericCompare(recVal, value) <= 0);
            case ("="): return (Table.GenericCompare(recVal, value) == 0);
            case ("!="):return (Table.GenericCompare(recVal, value) != 0);
            default:throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");
        }
    }
    public void createCSV() throws IOException {
        String path = "src\\main\\resources\\Basant\\" + this.tableName + "_Table.csv";
        FileWriter fw = new FileWriter(path);
        int size =0;

        for (int idx = 0; idx < table.size(); idx++) {

            tuple4 t = table.get(idx);
            Page p = (Page) DBApp.deserialize(tableName + "_" + t.id);
            size+=p.records.size();
            for (Page.Pair pair : p.records) {
                String str = "";
                Hashtable<String, Object> h = pair.row;
                Set<String> s = h.keySet();
                for (String o : s) {
                    str += h.get(o).toString() + ", ";
                }
                str += "\n";

                fw.write(str);
            }
            DBApp.serialize(tableName + "_" + t.id, p);
        }
        fw.write("Size : "+size );
        fw.close();
    }

    public static void createCSVSelect(Iterator it) throws IOException {
        String path = "src\\main\\resources\\Basant\\" + "Selection.csv";
        FileWriter fw = new FileWriter(path);
        while(it.hasNext()){
            Object rec=  it.next();
            String str = "";
            Set<String> s = ((Hashtable)rec).keySet();
            for (String o : s) {
                str += ((Hashtable)rec).get(o).toString() + ", ";
            }
            str += "\n";
            fw.write(str);
        }
        fw.close();
    }

    public static class tuple4 implements Serializable {
        Double id;
        transient Page page;
        Object min;
        Object max;

        public tuple4(Double id, Page page, Object min, Object max) {
            this.id = id;
            this.page = page;
            this.max = max;
            this.min = min;
        }
        public String print(String tableName) {
            Page p = (Page) DBApp.deserialize(tableName + "_" + id);
            String res =p.toString();
            DBApp.serialize(tableName + "_" + id,p);
            return res;
        }
    }

}
