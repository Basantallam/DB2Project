import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

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

        Object[] temp = new Vector[10];// todo of type
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
        char[] c= s.toCharArray();
        long res= 0;
        for (int i = 0; i <c.length ; i++) {
            long h = DBApp.code.get(c[i])==null?64:(long) DBApp.code.get(c[i]);

            res+= h*Math.pow(63,c.length-i-1);
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

    public int[] getCellCoordinates(Hashtable<String, Object> values) {
        Hashtable<String, Object> colValues = checkformatall(arrangeHashtable(values));
        int[] coordinates = new int[colValues.size()];

        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            Object min = ranges.get(colName).min;
            Object max = ranges.get(colName).max;

            Object value = colValues.get(colName);
            if (value == null) {
                coordinates[i]=0;
            }
            else {
                int idx = (value instanceof Long|| value instanceof Date) ? ( getIdxLong(min, max, value))
                        : getIdxDouble((double)min, (double)max, (double)value);
                coordinates[i]=idx;
            }

        }
        return coordinates;
    }

    private int getIdxDouble(double min, double max, double value) {
        double cellWidth = ((max- min) + 1) / 10.0; //range el cell kam raqam
        return (int)( Math.floor((value- min) / (cellWidth)));
    }

    private int getIdxLong(Object minimum, Object maximum, Object valueToAdd) {
        long min; long max; long value;
        if (minimum instanceof Date){
            min= ((Date) minimum).getTime();max=((Date)maximum).getTime();value=((Date)valueToAdd).getTime();
        }
        else{
            min = (long) minimum; max= (long) maximum;value= (long) valueToAdd;
        }
        long cellWidth = ((max- min) + 1) / 10; //range el cell kam raqam
        int idx = (int) ((value- min) / (cellWidth)); // O(1)
//        if ((value- min) % (cellWidth) > 0) {
//            idx++; //ceil //I think floor
//        }
        return idx;
    }

    public Vector<Integer> getCellsCoordinates(Hashtable<String, Object> values) {
        Vector<Integer> coordinates = new Vector<Integer>();

        Set<String> set = values.keySet();
        int IndexDimension = columnNames.size();
        Hashtable<String, Object> colValues = new Hashtable<String, Object>();

        for (int ptr = 0; ptr < IndexDimension; ptr++) {
            String col = columnNames.get(ptr);
            if (set.contains(col)) {
                colValues.put(col, values.get(col));
                String colName = columnNames.get(ptr);
                Object min = ranges.get(colName).min;
                Object max = ranges.get(colName).max;
                Object value = colValues.get(colName);
                int idx= (value instanceof Long|| value instanceof Date) ? ( getIdxLong(min, max, value))
                        : getIdxDouble((double)min, (double)max, (double)value);
                coordinates.add(idx);
            } else {
                coordinates.add(-1);
            }
        }
        return coordinates;
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
                extracted.put(col, null);
            }
        }
        return extracted;
    }
    public void updateAddress(Hashtable<String, Object> row, Double oldId, Double newId) {
        int[] cellIdx = getCellCoordinates(row);
        Vector<BucketInfo> cell = getCell(cellIdx);
        Object searchKey = row.get(columnNames.get(0));
        BucketInfo bi = cell.get(BinarySearchCell(cell, searchKey, cell.size() - 1, 0));
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
        Hashtable<String, Object> arrangedHash = arrangeHashtable(row);
        b.updateAddress(oldId, newId, arrangedHash);
        DBApp.serialize(tableName + "_b_" + bi.id, b);
    }
    public int BinarySearchCell(Vector<BucketInfo> cell, Object searchKey, int hi, int lo) {
        //binary search within one cell
        int mid = (hi + lo + 1) / 2;
        if (lo >= hi)
            return mid;
        if (Table.GenericCompare(cell.get(mid).min, searchKey) < 0)
            return BinarySearchCell(cell, searchKey, hi, mid);
        else
            return BinarySearchCell(cell, searchKey, mid - 1, lo);
    }
    public void insert(Hashtable<String, Object> colNameValue, Double id) {
        int[] cellIdx = getCellCoordinates(colNameValue);
        Vector<BucketInfo> cell = getCell(cellIdx);
        int bucketInfoIdx;
        BucketInfo foundBI;
        Bucket b;
        if(cell.size()==0){
            bucketInfoIdx=0;
            foundBI=new BucketInfo();
            cell.add(foundBI);
            b=foundBI.bucket;
        }else {
            bucketInfoIdx = BinarySearchCell(cell, colNameValue.get(columnNames.get(0)), 0, cell.size() - 1);
            foundBI = cell.get(bucketInfoIdx);
            b = (Bucket) DBApp.deserialize(tableName + "_b_" + foundBI.id);
        }
        Hashtable<String, Object> arrangedHash = arrangeHashtable(colNameValue);
        Bucket.Record returned = b.insert(arrangedHash, id);

        if (returned == null || !(returned.values.get(columnNames.get(0)).equals(colNameValue.get(columnNames.get(0))))) {
            foundBI.max = b.records.lastElement().values.get(columnNames.get(0));
            foundBI.min = b.records.firstElement().values.get(columnNames.get(0));
        }
        DBApp.serialize(tableName + "_b_" + foundBI.id, b);
        foundBI.size++;
        if (returned != null) {
            foundBI.size--;
            boolean create = true;
            if (cell.size() - 1 > bucketInfoIdx) {
                int nxtIdx = bucketInfoIdx + 1;
                Bucket nxtBucket = (Bucket) DBApp.deserialize(tableName + "_b_" + cell.get(nxtIdx).id);
                if (!nxtBucket.isFull()) {
                    create = false;
                    nxtBucket.insert(returned.values, id);
                }
                cell.get(nxtIdx).min = returned.values.get(columnNames.get(0));
                DBApp.serialize(tableName + "_b_" + cell.get(nxtIdx).id, nxtBucket);
            }

            if (create) {

                BucketInfo newBI = new BucketInfo();
                newBI.bucket.insert(returned.values, id);
                newBI.size++;
                cell.insertElementAt(newBI, bucketInfoIdx + 1);
                DBApp.serialize(tableName + "_b_" + newBI.id, newBI.bucket);
            }
        }
    }
    public Vector<BucketInfo> getCell(int[]  cellIdx) {

        Object cell = grid[(Integer) cellIdx[0]];
        for (int i = 1; i < cellIdx.length; i++) {
            int x = (Integer) cellIdx[i];
            Object y = ((Object[]) cell)[x];
            cell = y;
        }
        return (Vector<BucketInfo>) cell;
    }

    private Hashtable<String, Object> checkformatall(Hashtable<String, Object> colNameValue) {
        Hashtable<String,Object> parsed = (Hashtable<String, Object>) colNameValue.clone();
        for(String i:parsed.keySet())
            if(parsed.get(i) instanceof String) {
                parsed.replace(i, parseString((String) parsed.get(i)));
            }

        return parsed;
    }

    public void update(Hashtable<String, Object> oldRow, Hashtable<String, Object> newRow,
                       Hashtable<String, Object> updatedValues, double pageId) {
        Boolean update = false;
        for (String s : columnNames)
            if (updatedValues.containsKey(s)) { //todo if ranges different
                update = true;
                break;
            }
        if (!update)
            return;
        delete(oldRow, pageId);
        insert(newRow, pageId);
    }

    public void delete(Hashtable<String, Object> row, double pageId) {
        int[] cellIdx = getCellCoordinates(row);
        Vector<BucketInfo> cell = getCell(cellIdx);
        Object searchKey = row.get(columnNames.get(0));
        BucketInfo bi = cell.get(BinarySearchCell(cell, searchKey, cell.size() - 1, 0));
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
        Hashtable<String, Object> arrangedHash = arrangeHashtable(row);
        b.delete(arrangedHash,pageId);
        DBApp.serialize(tableName + "_b_" + bi.id, b);
    }
    public Vector<Double> narrowPageRange(Hashtable<String, Object> colNameValue) {
        int[] cellIdx = getCellCoordinates(colNameValue);
        Vector<BucketInfo> cell = getCell(cellIdx);
        int bucketInfoIdx = BinarySearchCell(cell, colNameValue.get(columnNames.get(0)), 0, cell.size() - 1);
        BucketInfo foundBI = cell.get(bucketInfoIdx);
        Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + foundBI.id);
        Vector res = b.getInsertCoordinates(colNameValue);
        DBApp.serialize(tableName + "_b_" + foundBI.id,b);
        return res;
    }
    public int getSize() {
        return columnNames.size();
    }
    public static void main(String[] args) {
//		String[] stringarr = { "boo", "bar", "foo", "lol" }; // n=4
//		Index idx = new Index("tablename",stringarr, new Hashtable<>(), this.table);
//		System.out.println((Arrays.deepToString(idx.grid)));// 4d
//		System.out.println(Arrays.deepToString((Object[]) idx.grid[0]));// 3d
//		System.out.println(Arrays.deepToString((Object[]) (((Object[]) idx.grid[0])[0]))); // 2d
//		System.out.println(Arrays.deepToString(((Object[]) (((Object[]) idx.grid[0])[0])))); // 1d
    }

    public Vector<Double> delete(Hashtable<String, Object> columnNameValue) {
        Vector<Double>pages=new Vector<>();
        Vector<Integer> coordinates = getCellsCoordinates(columnNameValue);
        Vector<Vector<BucketInfo>> cells=new Vector<>();
        if (coordinates.get(0) == -1) {
            for (int i=0;i<grid.length;i++) {
                cells.add(helper(coordinates,1, (Object[]) grid[i]));
            }
        }else{
            cells.add(helper(coordinates,1, (Object[]) grid[coordinates.get(0)]));
        }
        for (Vector<BucketInfo> cell:cells) {
            Hashtable<String, Object> arrangedHash = arrangeHashtable(columnNameValue);
            if(columnNameValue.keySet().contains(clusteringCol)){
                Object searchKey = columnNameValue.get(clusteringCol);
                BucketInfo bi = cell.get(BinarySearchCell(cell, searchKey, cell.size() - 1, 0));
                Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
                Vector<Double>p=b.deleteI(arrangedHash);
                for (double x:p) {
                    pages.add(x);
                }
                DBApp.serialize(tableName + "_b_" + bi.id, b);
            }else {
                for (BucketInfo bi : cell) {
                    Bucket b = (Bucket) DBApp.deserialize(tableName + "_b_" + bi.id);
                    Vector<Double>p=b.deleteI(arrangedHash);
                    for (double x:p) {
                        pages.add(x);
                    }
                    DBApp.serialize(tableName + "_b_" + bi.id, b);
                }
            }
        }return pages;
    }
    private Vector<BucketInfo> helper(Vector<Integer> coordinates, int ptr,Object grid) {
        if(ptr>=coordinates.size() && ptr <coordinates.size()+10){
            Vector<BucketInfo>cell= (Vector<BucketInfo>)((Object[])grid)[ptr-coordinates.size()];
            return cell;
        }
        else if(ptr==coordinates.size()-1){
            if(coordinates.get(ptr)==-1){
                for(int i=0;i<10;i++) helper(coordinates,ptr+i,grid);
            }else {
                Vector<BucketInfo>cell= (Vector<BucketInfo>)((Object[])grid)[coordinates.get(ptr)];
                return cell;
            }
        }else {
            Object[]cell=((Object[])grid);
            int x=coordinates.get(ptr);
            if (coordinates.get(x) == -1) {

                for (int i = 0; i < cell.length; i++) {
                    Object y = ((Object[]) cell)[i];
                    grid = y;
                    return helper(coordinates, ptr + 1, grid);
                }
            }else{
                return helper(coordinates, ptr + 1, cell[x]);
            }
        }return null;
    }

    public Vector lessThan(SQLTerm term) {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        int[] LastCellCoordinates = this.getCellCoordinates(hashtable);
        //supposedly no partial queries ya3ni mafeesh nulls fel hashtable?? wala a set el limit le eih?
        Vector res = loopUntilExclusive(LastCellCoordinates,term);

        return res;
    }
    public Vector lessThanOrEqual(SQLTerm term) {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        int[] LastCellCoordinates = this.getCellCoordinates(hashtable);
        //supposedly no partial queries ya3ni mafeesh nulls fel hashtable?? wala a set el limit le eih?
        Vector res = loopUntilInclusive(LastCellCoordinates,term);

        return res;
    }
    public Vector<Bucket.Record> loopUntilExclusive(int[]limits, SQLTerm term){
        //includes all records in last cell just before the ones satisfying the cond
        Vector<Bucket.Record> ref=new Vector<Bucket.Record>();
        loopUntil(new int[limits.length],limits,0,ref);
        Vector <BucketInfo> lastCell= getCell(limits);
        for(BucketInfo bi :lastCell){
            for(Bucket.Record r:bi.bucket.records){
                Object recordVal= r.values.get(term._strColumnName);
                if(Table.GenericCompare(recordVal,term._objValue)<0){
                    ref.add(r);
                }
            }
        }
        return ref;
    }
    public Vector<Bucket.Record> loopUntilInclusive(int[]limits,SQLTerm term){
        //includes records in last cell satisfying cond
        Vector<Bucket.Record> ref=new Vector<Bucket.Record>();
        loopUntil(new int[limits.length],limits,0,ref);
        Vector <BucketInfo> lastCell= getCell(limits);
        for(BucketInfo bi :lastCell){
            for(Bucket.Record r:bi.bucket.records){
                Object recordVal= r.values.get(term._strColumnName);
                if(Table.GenericCompare(recordVal,term._objValue)<=0){
                    ref.add(r);
                }
            }
        }
        return ref;
    }
    public void loopUntil(int[] curr,int[] limits,int depth, Vector<Bucket.Record> accumulated){

        if(depth==limits.length){
            Vector<BucketInfo> cell =getCell(curr);
            for(BucketInfo bi :cell){
                for(Bucket.Record r:bi.bucket.records){
                    accumulated.add(r);
                }
            }
            return;
        }
        for(int i=0;i<limits[depth];i++){ //excludes el last cell(limits)
            int[] newCurr =curr.clone();
            newCurr[depth]=i;
            loopUntil(newCurr,limits,depth+1,accumulated);
        }
    }

    public Vector greaterThan(SQLTerm term) {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        int[] FirstCellCoordinates = this.getCellCoordinates(hashtable);
        //supposedly no partial queries ya3ni mafeesh nulls fel hashtable?? wala a set el limit le eih?
        return this.loopFromExclusive(FirstCellCoordinates,term);
    }

    public Vector<Bucket.Record> loopFromInclusive(int[] start,SQLTerm term){
        Vector<Bucket.Record> ref=new Vector<Bucket.Record>();
        Vector <BucketInfo> firstCell = getCell(start);
        for(BucketInfo bi :firstCell){
            for(Bucket.Record r:bi.bucket.records){
                Object recordVal= r.values.get(term._strColumnName);
                if(Table.GenericCompare(recordVal,term._objValue)>=0){
                    ref.add(r);
                }
            }
        }
        loopUntil(pLusOne(start),pLusOne(this.getEnd()),0,ref);
        //bazawed ones 3ala kol el array bta3 getEnd 3ashan loop until bet exclude akher cell
        //bazawed ones 3ala start 3ashan acheck each record individually bet satisfy wala la2
        return ref;
    }


    private int[] pLusOne(int[] arr) {
        int [] arrnew=arr.clone();
        for(int i=0;i<arr.length;i++){
            arrnew[i] = arr[i]+1;
        }
        return arrnew;
    }

    private int[] getEnd() {
        //todo returns laaaast coordinates in grid
        return null;
    }

    public Vector<Bucket.Record> loopFromExclusive(int[] start,SQLTerm term){
        Vector<Bucket.Record> ref=new Vector<Bucket.Record>();
        Vector <BucketInfo> firstCell = getCell(start);
        for(BucketInfo bi :firstCell){
            for(Bucket.Record r:bi.bucket.records){
                Object recordVal= r.values.get(term._strColumnName);
                if(Table.GenericCompare(recordVal,term._objValue)>0){
                    ref.add(r);
                }
            }
        }
        loopUntil(pLusOne(start),pLusOne(this.getEnd()),0,ref);
        //bazawed ones 3ala kol el array bta3 getEnd 3ashan loop until bet exclude akher cell
        //bazawed ones 3ala start 3ashan acheck each record individually bet satisfy wala la2
        return ref;
    }
    public Vector greaterThanOrEqual(SQLTerm term) {
        Hashtable<String, Object> hashtable = new Hashtable<>();
        hashtable.put(term._strColumnName, term._objValue);
        int[] FirstCellCoordinates = this.getCellCoordinates(hashtable);
        //supposedly no partial queries ya3ni mafeesh nulls fel hashtable?? wala a set el limit le eih?
        return this.loopFromInclusive(FirstCellCoordinates,term);
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
            this.bucket = new Bucket(id, clusteringCol,columnNames.get(0));
            this.max = null;
            this.min = null;
        }
    }
}