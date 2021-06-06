import java.io.File;
import java.io.Serializable;
import java.util.*;

public class Index implements Serializable {
    int serialID;
    String tableName;
    Vector<String> columnNames;
    Hashtable<String, DBApp.minMax> ranges;
    Object[] grid;
    String clusteringCol;

    public Index(String tableName, String[] columnNames, Hashtable<String, DBApp.minMax> ranges,
                 Vector<Table.tuple4> table, String clusteringCol) {
        this.tableName = tableName;
        this.clusteringCol = clusteringCol;
        fillColumnNames(columnNames);

        int n = columnNames.length;

        this.ranges = ranges; // O(1) to find cell index

        Object[] temp = new Vector[10];
        Object[] temp1 = new Object[10];
        for (int i = 0; i < 10; i++) {
            temp[i] = new Vector<BucketInfo>();
        }
        for (int i = 1; i < n; i++) {
            for (int j = 0; j < 10; j++)
                temp1[j] = deepClone(temp);
            temp = temp1;
            temp1 = new Object[10];
        }

        this.grid = temp;
        this.fill(table);

    }
    private void fillColumnNames(String[] columnNames) {
        this.columnNames = new Vector<>();
        for (String s : columnNames) {
            if (s.equals(clusteringCol))
                this.columnNames.add(0, s);
            else
                this.columnNames.add(s);
        }
    }
    public static long parseString(String s) {
        char[] c = s.toCharArray();
        long res = 0;
        for (int i = 0; i < c.length; i++) {
            long h = DBApp.code.get(c[i]) == null ? 64 : (long) DBApp.code.get(c[i]);

            res += h * Math.pow(63, c.length - i - 1);
        }
        return res;
    }
    private Vector<DBApp.minMax> arrangeRanges(Hashtable<String, DBApp.minMax> ranges) {
        Set<String> set = ranges.keySet();
        int IndexDimension = columnNames.size();
        Vector<DBApp.minMax> arrangedRanges = new Vector<DBApp.minMax>();

        for (int ptr = 0; ptr < IndexDimension; ptr++) {
            String col = columnNames.get(ptr);
            if (set.contains(col))
                arrangedRanges.add(ranges.get(col));
        }
        return arrangedRanges;
    }
    public Object deepClone(Object[] org) {
        Object[] clone = new Object[org.length];
        if (org[0] instanceof Object[]) {
            for (int i = 0; i < org.length; i++)
                clone[i] = deepClone((Object[]) org[i]);
        } else
            clone = org.clone();

        return clone;
    }
    private void fill(Vector<Table.tuple4> table) {
        for (Table.tuple4 t : table) {
            double pageId = t.id;
            Page page = (Page) DBApp.deserialize(tableName + "_" + pageId);
            for (Page.Pair r : page.records)
                insert(r.row, pageId);
            DBApp.serialize(tableName + "_" + pageId, page);
        }
    }
    public int[] getCellCoordinates(Hashtable<String, Object> values, boolean nine) {// added boolean for range queries
        Hashtable<String, Object> colValues = checkformatall(arrangeHashtable(values));
        int[] coordinates = new int[columnNames.size()];

        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            Object min = ranges.get(colName).min;
            Object max = ranges.get(colName).max;

            Object value = colValues.get(colName);
            if (value == null) {
                coordinates[i] = nine ? 9 : 0; //depending on bool ha7ot 0 wala 9
            } else {
                int idx = (value instanceof Long || value instanceof Date) ? (getIdxLong(min, max, value))
                        : getIdxDouble((double) min, (double) max, (double) value);
                coordinates[i] = idx;
            }

        }
        return coordinates;
    }
    private int getIdxDouble(double min, double max, double value) {
        double cellWidth = ((max - min) + 1) / 10.0; //range el cell kam raqam
        return (int) (Math.floor((value - min) / (cellWidth)));
    }
    private int getIdxLong(Object minimum, Object maximum, Object valueToAdd) {
        long min;
        long max;
        long value;
        if (minimum instanceof Date) {
            min = ((Date) minimum).getTime();
            max = ((Date) maximum).getTime();
            value = ((Date) valueToAdd).getTime();
        } else {
            min = (long) minimum;
            max = (long) maximum;
            value = (long) valueToAdd;
        }
        long cellWidth = ((max - min) + 1) / 10; //range el cell kam raqam
        int idx = (int) ((value - min) / (cellWidth)); // O(1)
//        if ((value- min) % (cellWidth) > 0) {
//            idx++; //ceil //I think floor
//        }
        return idx;
    }
    public Hashtable<String, Object> arrangeHashtable(Hashtable<String, Object> values) {
        //takhod kol el record teraga3 el values eli fel Index columns bass

        Set<String> set = values.keySet();
        int IndexDimension = columnNames.size();
        Hashtable<String, Object> extracted = new Hashtable<String, Object>();

        for (int ptr = 0; ptr < IndexDimension; ptr++) {
            String col = columnNames.get(ptr);
            if (set.contains(col)) {
                extracted.put(col, values.get(col));
            } else {
                //todo aragaha?:
//                extracted.put(col, null);
            }
        }
        return extracted;
    }
    public void updateAddress(Hashtable<String, Object> row, Double oldId, Double newId) {
        int[] cellIdx = getCellCoordinates(row, false);
        Vector<BucketInfo> cell = getCell(cellIdx);
        Object searchKey = row.get(columnNames.get(0));
        BucketInfo bi = cell.get(BinarySearchCell(cell, searchKey, cell.size() - 1, 0));
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
        Hashtable<String, Object> arrangedHash = arrangeHashtable(row);
        b.updateAddress(oldId, newId, arrangedHash);
        DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
    }
    public int BinarySearchCell(Vector<BucketInfo> cell, Object searchKey, int hi, int lo) {
        //binary search within one cell returns index of bucket
        int mid = (hi + lo + 1) / 2;
        if (lo >= hi)
            return mid;
        if (Table.GenericCompare(cell.get(mid).min, searchKey) < 0)
            return BinarySearchCell(cell, searchKey, hi, mid);
        else
            return BinarySearchCell(cell, searchKey, mid - 1, lo);
    }
    public void insert(Hashtable<String, Object> colNameValue, Double id) {
        int[] cellIdx = getCellCoordinates(colNameValue, false);
        Vector<BucketInfo> cell = getCell(cellIdx);
        int bucketInfoIdx;
        BucketInfo foundBI;
        Bucket b;
        if (cell.size() == 0) {
            bucketInfoIdx = 0;
            foundBI = new BucketInfo();
            cell.add(foundBI);
            b = foundBI.bucket;
        } else {
            bucketInfoIdx = BinarySearchCell(cell, colNameValue.get(columnNames.get(0)),  cell.size() - 1,0);
            foundBI = cell.get(bucketInfoIdx);
            b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + foundBI.id);
        }
        Hashtable<String, Object> arrangedHash = arrangeHashtable(colNameValue);
        Bucket.Record returned = b.insert(arrangedHash, id);

        if (returned == null || !(returned.values.get(columnNames.get(0)).equals(colNameValue.get(columnNames.get(0))))) {
            foundBI.max = b.records.lastElement().values.get(columnNames.get(0));
            foundBI.min = b.records.firstElement().values.get(columnNames.get(0));
        }
        DBApp.serialize(tableName + "_" + columnNames + "_" + foundBI.id, b);
        foundBI.size++;
        if (returned != null) {
            foundBI.size--;
            boolean create = true;
            if (cell.size() - 1 > bucketInfoIdx) {
                int nxtIdx = bucketInfoIdx + 1;
                Bucket nxtBucket = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + cell.get(nxtIdx).id);
                if (!nxtBucket.isFull()) {
                    create = false;
                    nxtBucket.insert(returned.values, id);
                }
                cell.get(nxtIdx).min = returned.values.get(columnNames.get(0));
                DBApp.serialize(tableName + "_" + columnNames + "_" + cell.get(nxtIdx).id, nxtBucket);
            }

            if (create) {

                BucketInfo newBI = new BucketInfo();
                newBI.bucket.insert(returned.values, id);
                newBI.size++;
                cell.insertElementAt(newBI, bucketInfoIdx + 1);
                DBApp.serialize(tableName + "_" + columnNames + "_" + newBI.id, newBI.bucket);
            }
        }
    }
    public Vector<BucketInfo> getCell(int[] cellIdx) {
        Object cell = grid[cellIdx[0]];
        for (int i = 1; i < cellIdx.length; i++) {
            int x = cellIdx[i];
            Object y = ((Object[]) cell)[x];
            cell = y;
        }
        return (Vector<BucketInfo>) cell;
    }
    private Hashtable<String, Object> checkformatall(Hashtable<String, Object> colNameValue) {
        Hashtable<String, Object> parsed = (Hashtable<String, Object>) colNameValue.clone();
        for (String i : parsed.keySet())
            if (parsed.get(i) instanceof String) {
                parsed.replace(i, parseString((String) parsed.get(i)));
            }

        return parsed;
    }
    public void update(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow,
                       Hashtable<String, Object> updatedValues, double pageId) {
        Boolean update = false;
        for (String s : columnNames)
            if (updatedValues.containsKey(s)) {
                update = true;
                break;
            }
        if (!update)
            return;
        delete(oldRow, pageId);
        insert(newRow, pageId);
    }
    public void delete(Hashtable<String, Object> row, double pageId) {
        int[] cellIdx = getCellCoordinates(row, false);
        Vector<BucketInfo> cell = getCell(cellIdx);
        Object searchKey = row.get(columnNames.get(0));
        BucketInfo bi = cell.get(BinarySearchCell(cell, searchKey, cell.size() - 1, 0));
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
        Hashtable<String, Object> arrangedHash = arrangeHashtable(row);
        b.delete(arrangedHash, pageId);
        deleteRefactorBucket(b,cell,bi);
    }
    private void deleteRefactorBucket(Bucket b, Vector<BucketInfo> cell, BucketInfo bi) {
        if (b.isEmpty()) {
            cell.remove(bi);
            new File("src/main/resources/data/" + tableName + "_" + columnNames + "_" + bi.id+ ".ser").delete();
        } else {
            bi.max = b.records.lastElement().values.get(columnNames.get(0));
            bi.min = b.records.firstElement().values.get(columnNames.get(0));
            DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
        }
    }
    public Vector<Double> narrowPageRange(Hashtable<String, Object> colNameValue) {
        int[] cellIdx = getCellCoordinates(colNameValue, false);
        Vector<BucketInfo> cell = getCell(cellIdx);
        if(cell.size()==0){
            Vector r =new Vector<>();
            double x=-1;
             r.add(x); r.add(x);
             return r;
        }
        int bucketInfoIdx = BinarySearchCell(cell, colNameValue.get(columnNames.get(0)), cell.size() - 1,0);
        BucketInfo foundBI = cell.get(bucketInfoIdx);
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + foundBI.id);
        Vector res = b.getInsertCoordinates(colNameValue);
        DBApp.serialize(tableName + "_" + columnNames + "_" + foundBI.id, b);
        return res;
    }
    public int getSize() {
        return columnNames.size();
    }

    public HashSet<Double> delete(Hashtable<String, Object> columnNameValue) {
        HashSet<Double> pages = new HashSet<>();
        Vector<StartEnd> coordinates = getCellsCoordinates(columnNameValue,null);
        Vector<Vector<BucketInfo>> cells = new Vector<>();
        getAllCells(coordinates,0, grid,cells );
        for (Vector<BucketInfo> cell : cells) {
            Hashtable<String, Object> arrangedHash = arrangeHashtable(columnNameValue);
            if (columnNameValue.containsKey(columnNames.get(0))) {
                Object searchKey = columnNameValue.get(columnNames.get(0));
                int idx=BinarySearchCell(cell, searchKey, cell.size() - 1, 0);
                for (int i = idx; i < cell.size(); i++) {
                    BucketInfo bi = cell.get(idx);
                    if(Table.GenericCompare(bi.min, searchKey) > 0)break;
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    pages.addAll(b.deleteI(arrangedHash));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
            } else {
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    pages.addAll(b.deleteI(arrangedHash));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
            }
        }
        return pages;
    }
    private void getAllCells(Vector<StartEnd> coordinates, int ptr, Object grid, Vector<Vector<BucketInfo>> cells) {
        int i = coordinates.get(ptr).start; int end = coordinates.get(ptr).end;
        if (ptr == coordinates.size() - 1) {
            for (; i < end; i++)
                if(((Vector<BucketInfo>) ((Object[]) grid)[i]).size()!=0)
                    cells.add((Vector<BucketInfo>) ((Object[]) grid)[i]);
        } else {
            Object[] cell = ((Object[]) grid);
            for (; i < end; i++) {
                Object y = ((Object[]) cell)[i];
                grid =  y;
                getAllCells(coordinates, ptr + 1, grid,cells );
            }
        }
    }
    public HashSet<Double> lessThan(SQLTerm term) { //index traversal
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        Hashtable<String, String> operator = new Hashtable<>();
        operator.put(term._strColumnName, term._strOperator);

        Vector<Vector<BucketInfo>> cells = new Vector<>();
        getAllCells(getCellsCoordinates(hashtable,operator),0, grid,cells );
        String indexClustCol = columnNames.get(0);
        HashSet<Double> res= new HashSet<>();
        for (Vector<BucketInfo> cell : cells) {
            if (hashtable.containsKey(indexClustCol)) //checks if within el cells is sorted on queried col
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    for (Bucket.Record r:b.records)
                        if (b.checkCond(r,term)) res.add(r.pageid);
                        else break;
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
            else //linear search within cell
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    res.addAll(b.filterBucket(term));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
        }
        return (res);
    }



    private void filterCell(Vector<BucketInfo> cell, SQLTerm term, HashSet<Double> result) {
        // loops on cell record by record adds records that match condition
        for (BucketInfo bi : cell) {
            Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
            result.addAll(b.filterBucket(term));
            DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id,b);
        }
    }

    public HashSet<Double> greaterThan(SQLTerm term) throws DBAppException {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        Hashtable<String, String> operator = new Hashtable<>();
        operator.put(term._strColumnName, term._strOperator);

        Vector<Vector<BucketInfo>> cells = new Vector<>();
        getAllCells(getCellsCoordinates(hashtable,operator),0, grid,cells );
        HashSet<Double> res= new HashSet<>();
        for (Vector<BucketInfo> cell : cells) {
           //linear search within cell could've done binary search if clustering col bass risky seeka
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    res.addAll(b.filterBucket(term));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
        }
        return (res);
    }

    public HashSet<Double> equalSelect(SQLTerm term) {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);

        HashSet<Double> pages = new HashSet<Double>();
        Vector<StartEnd> coordinates = getCellsCoordinates(hashtable,null);
        Vector<Vector<BucketInfo>> cells = new Vector<>();
        getAllCells(coordinates,0, grid,cells );
        for (Vector<BucketInfo> cell : cells) {
            if (hashtable.containsKey(columnNames.get(0))) { //checks if within el cells is sorted on queried col
                Object searchKey = hashtable.get(columnNames.get(0));
                int idx=BinarySearchCell(cell, searchKey, cell.size() - 1, 0);
                for (int i = idx; i < cell.size(); i++) {
                    BucketInfo bi = cell.get(idx);
                    if(Table.GenericCompare(bi.min, searchKey) > 0) break;
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    pages.addAll(b.filterBucket(term));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
            } else { //linear search within cell
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);
                    pages.addAll(b.filterBucket(term));
                    DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
                }
            }
        }
        return pages;
    }

    public HashSet<Double> andSelect(SQLTerm term1, SQLTerm term2) {
        Hashtable<String, Object> columnNameValue= new Hashtable<>();
        Hashtable<String, String> columnOperators= new Hashtable<>();
        if(!term1._strOperator.equals("!=")){
            columnNameValue.put(term1._strColumnName,term1._objValue);
            columnOperators.put(term1._strColumnName,term1._strOperator);
        } if(!term2._strOperator.equals("!=")){
            columnNameValue.put(term2._strColumnName,term2._objValue);
            columnOperators.put(term2._strColumnName,term2._strOperator);
        }
        Vector<StartEnd> coordinates = getCellsCoordinates(columnNameValue,columnOperators);
        Vector<Vector<BucketInfo>> cells = new Vector<>();
        getAllCells(coordinates,0, grid,cells);
        HashSet<Double> pages = new HashSet<>();
        boolean col1IsInIndex=columnNames.contains(term1._strColumnName);
        boolean col2IsInIndex=columnNames.contains(term2._strColumnName);
        for (Vector<BucketInfo> cell : cells) {
            for (BucketInfo bi : cell) {
                Bucket b = (Bucket) DBApp.deserialize(tableName + "_" + columnNames + "_" + bi.id);

                if(col1IsInIndex&&col2IsInIndex) pages.addAll(b.condSelect(term1,term2)); //translating buckets to page IDs
                else if(col1IsInIndex) pages.addAll(b.condSelect(term1));
                else if(col2IsInIndex) pages.addAll(b.condSelect(term2));

                DBApp.serialize(tableName + "_" + columnNames + "_" + bi.id, b);
            }
        }
        return pages;

    }

    private Vector<StartEnd> getCellsCoordinates(Hashtable<String, Object> columnNameValue, Hashtable<String, String> columnOperators) {
        Hashtable<String, Object> colValues = checkformatall(arrangeHashtable(columnNameValue));
        Vector<StartEnd> coordinates = new Vector<>();
        Set<String> set = colValues.keySet();
        int IndexDimension = columnNames.size();

        for (int ptr = 0; ptr < IndexDimension; ptr++) {
            String col = columnNames.get(ptr);
            if (set.contains(col)) {
                String colName = columnNames.get(ptr);
                String op = columnOperators==null?"=":columnOperators.get(colName);
                Object min = ranges.get(colName).min;
                Object max = ranges.get(colName).max;
                Object value = colValues.get(colName);
                int idx = (value instanceof Long || value instanceof Date) ? (getIdxLong(min, max, value))
                        : getIdxDouble((double) min, (double) max, (double) value);
                coordinates.add(getOperation(idx,op));
            } else {
                coordinates.add(new StartEnd(0,10));
            }
        }
        return coordinates;
    }

    private StartEnd getOperation(int idx, String operator) {
        switch (operator) {
            case (">"): case (">="):return new StartEnd(idx,10);
            case ("<"): case ("<="):return new StartEnd(0,idx+1);
            default: return new StartEnd(idx,idx+1);
        }
    }

    class BucketInfo implements Serializable {
        long id;
        int size;
        transient Bucket bucket;
        Object max;
        Object min;

        public BucketInfo() {
            this.size = 0;
            this.id = ++serialID;
            this.bucket = new Bucket(id, clusteringCol, columnNames.get(0));
            this.max = null;
            this.min = null;
        }
        public String toString(){
            return "id:"+id+"size "+size+" bucket "+bucket;
        }
    }
    class StartEnd{
        int start;
        int end;

        public StartEnd(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }
}