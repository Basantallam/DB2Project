import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket  implements Serializable {
    long id;
    Vector<tuple> Bucket;

    public Bucket(long id) {
        this.id=id;
        Bucket = new Vector<tuple>();
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
