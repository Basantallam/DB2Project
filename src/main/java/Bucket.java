import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket  implements Serializable {
    long id;
    Vector<tuple> bucket;

    public Bucket(long id) {
        this.id=id;
        bucket = new Vector<tuple>();
    }

    public void delete(Hashtable<String, Object> row, double pageId) {
    }

    public void insert(Hashtable<String, Object> colNameValue, Double id) {
    }


    private class tuple {
        Hashtable value;
        double pageid;

        public tuple(Hashtable value, double pageid) {
            this.value = value;
            this.pageid = pageid;
        }
    }
}
