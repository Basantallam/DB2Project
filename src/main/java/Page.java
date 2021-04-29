import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    Vector <Hashtable <String, Object>> records;
    Object max;
    Object min;

    //TODO class pages
    public Page(){
        records=new Vector<Hashtable<String,Object>>();
        //TODO set min and max values

    }

    public void insert(Hashtable<String, Object> colNameValue) throws DBAppException{//TODO

        //binary search or linear search within page?
    }

    public void update( String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {//TODO
        // update max/min

    }

    public void delete(Hashtable<String, Object> columnNameValue) throws DBAppException {//TODO
        // update max/min

    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

}
