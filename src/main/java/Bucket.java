import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Bucket implements Serializable {
    long id;
    Vector<Record> records;
    String clusteringCol;

    public Bucket(long id, String clust) {
        this.id = id;
        records = new Vector<Record>();
        clusteringCol = clust;
    }

    public Vector<Double> getInsertCoordinates(Hashtable<String, Object> row) {
        //		vector containing min and max pages
        //min el awel w ba3den hi
        Vector<Double> res = new Vector<Double>();
        double lo=-1; double hi = -1;
        Object pkValue = row.get(clusteringCol);
        int i = BinarySearch(pkValue, records.size() - 1, 0);
        if(i!=0 && Table.GenericCompare(records.get(i-1).values.get(clusteringCol),pkValue)<0 ){
            lo = records.get(i-1).pageid;
        }
        if(Table.GenericCompare(records.get(i).values.get(clusteringCol),pkValue)>0)hi=records.get(i).pageid;
        res.add(lo);res.add(hi);
    return res;
    }

    public void delete(Hashtable<String, Object> row, double pageId) {
        Object pkValue = row.get(clusteringCol);
        int i = BinarySearch(pkValue, records.size() - 1, 0);
        records.get(i).pageid=pageId;
    }

    public Record insert(Hashtable<String, Object> colNameValue, Double pageID) {
        Record newRecord = new Record(colNameValue, pageID);
        // binary search bucket should be sorted mesh ba insert w khalas
        //  records.add(newRecord);
        Object pkvalue = colNameValue.get(clusteringCol);
        if (this.isFull()) {
            if (Table.GenericCompare(records.lastElement().values.get(clusteringCol), pkvalue) < 0)
                return newRecord;
            else {
                int i = BinarySearch(pkvalue, records.size() - 1, 0); // if it is working
                records.insertElementAt(newRecord, i); // full capacity+1
                return records.remove(DBApp.capacity);
            }
        } else {
            int i = BinarySearch(pkvalue, records.size() - 1, 0);
            records.add(i, newRecord);
            return null;
        }

    }

    public int BinarySearch(Object searchkey, int hi, int lo) {
        int mid = (hi + lo) / 2;

        if (lo >= hi)
            return mid;

        if (Table.GenericCompare(records.get(mid).values.get(clusteringCol), searchkey) > 0)
            return BinarySearch(searchkey, mid, lo);
        else
            return BinarySearch(searchkey, hi, mid + 1);

    }

    public void updateAddress(double oldAddress, double newAddress, Hashtable<String, Object> values) {
        Object pkvalue = values.get(clusteringCol);
        int i = BinarySearch(pkvalue, records.size() - 1, 0);
        records.get(i).pageid=newAddress;


    }

    public boolean isFull() {
        return this.records.size() == DBApp.indexCapacity;
    }

    class Record {
        Hashtable values;
        double pageid;

        public Record(Hashtable<String, Object> value, double pageid) {
            this.values = value;
            this.pageid = pageid;
        }

    }
}
