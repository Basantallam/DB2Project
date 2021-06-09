import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements Serializable {
    long id;
    Vector<Record> records;
    String clusteringTable;
    String sortedIndex;

    public Bucket(long id, String clusteringCol, String sorted) {
        this.id = id;
        records = new Vector<Record>();
        this.clusteringTable = clusteringCol;
        this.sortedIndex=sorted;
    }
    public Vector<Double> getInsertCoordinates(Hashtable<String, Object> row) {
        //	vector containing min and max pages
        //min el awel w ba3den hi
        Vector<Double> res = new Vector<Double>();
        double lo=-1; double hi = -1;
        Object pkValue = row.get(clusteringTable);
        int i = BinarySearch(pkValue, records.size() - 1, 0);
        if(i!=0 && Table.GenericCompare(records.get(i-1).values.get(clusteringTable),pkValue)<=0 ){
            lo = records.get(i-1).pageid;
        }
        if(Table.GenericCompare(records.get(i).values.get(clusteringTable),pkValue)>=0)hi=records.get(i).pageid;
        res.add(lo);res.add(hi);
    return res;
    }
    public void delete(Hashtable<String, Object> row, double pageId) {
        Object clusterValue = row.get(sortedIndex);
        int i = BinarySearch(clusterValue, records.size() - 1, 0);
        records.get(i).pageid=pageId;
    }
    public Record insert(Hashtable<String, Object> colNameValue, Double pageID) {
        Record newRecord = new Record(colNameValue, pageID);
        // binary search bucket should be sorted mesh ba insert w khalas
        //  records.add(newRecord);
        Object clusterValue = colNameValue.get(sortedIndex);
        if (this.isFull()) {
            if (Table.GenericCompare(records.lastElement().values.get(sortedIndex), clusterValue) < 0)
                return newRecord;
            else {
                int i = BinarySearch(clusterValue, records.size() , 0); // if it is working
                records.insertElementAt(newRecord, i); // full capacity+1
                return records.remove(DBApp.indexCapacity);
            }
        } else {
            int i = BinarySearch(clusterValue, records.size() , 0);
            records.add(i, newRecord);
            return null;
        }

    }
    public int BinarySearch(Object searchkey, int hi, int lo) {
        int mid = (hi + lo) / 2;

        if (lo >= hi)
            return mid;
        if (Table.GenericCompare(records.get(mid).values.get(sortedIndex), searchkey) >= 0)// should be sortedIndex not clusterCol might be there is no clustering col
            return BinarySearch(searchkey, mid, lo);
        else
            return BinarySearch(searchkey, hi, mid + 1);

    }
    public void updateAddress(double oldAddress, double newAddress, Hashtable<String, Object> values) {
        Object sortingValue = values.get(sortedIndex);
        int i = BinarySearch(sortingValue, records.size() - 1, 0);
        if(records.get(i).pageid==oldAddress)
            records.get(i).pageid=newAddress;
    }
    public boolean isFull() {
        return this.records.size() == DBApp.indexCapacity;
    }
    public HashSet<Double> deleteI(Hashtable<String, Object> columnNameValue) {
        HashSet<Double> pages=new HashSet();
        Object sortingValue = columnNameValue.get(sortedIndex);
        if(sortingValue==null){
            for (Record r:records)
                for (String s: columnNameValue.keySet())
                    if(columnNameValue.get(s).equals(r.values.get(s)))
                        pages.add(r.pageid);

        }else{
            int i = BinarySearch(sortingValue, records.size() - 1, 0);
            for (int j = i; j < records.size(); j++) {
                if(!records.get(j).values.get(sortedIndex).equals(sortingValue))break;
                for (String s: columnNameValue.keySet())
                    if(columnNameValue.get(s).equals(records.get(j).values.get(s)))
                        pages.add(records.get(j).pageid);
            }
        }return pages;
    }

    static boolean checkCond(Bucket.Record record, SQLTerm term) {
        switch (term._strOperator){
            case("<"):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)<0;
            case("<="):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)<=0;
            case(">"):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)>0;
            case(">="):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)>=0;
            case("="):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)==0;
            case("!="):
                return Table.GenericCompare(record.values.get(term._strColumnName),term._objValue)!=0;
            default: return false;
        }
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public HashSet<Double> filterBucket(SQLTerm term) {
        HashSet<Double> res= new HashSet<>();
        for (Record r:records){
            if (checkCond(r,term))
                    res.add(r.pageid);
        }
        return  res;
    }


    public HashSet<Double> getPageIds() {
        HashSet<Double> res= new HashSet<>();
        for (Record r:records)res.add(r.pageid);
        return res;
    }

    public Collection<Double> condSelect(SQLTerm term1, SQLTerm term2) {//add or w xor?
        HashSet<Double> res= new HashSet<>();
        for (Record r:records){
            if (checkCond(r,term1)&& checkCond(r,term2))
                res.add(r.pageid);
        }
        return  res;
    }

    public Collection<Double> condSelect(SQLTerm term) {
        HashSet<Double> res= new HashSet<>();
        for (Record r:records){
            if (checkCond(r,term))
                res.add(r.pageid);
        }
        return  res;
    }

    class Record implements Serializable{
        Hashtable<String, Object> values;
        double pageid;

        public Record(Hashtable<String, Object> value, double pageid) {
            this.values = value;
            this.pageid = pageid;
        }

        @Override
        public String toString() {
            return "Record{" +
                    "values=" + values +
                    ", pageid=" + pageid +
                    '}';
        }
    }
}
