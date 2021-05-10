import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket  implements Serializable {
    Vector<tuple> Bucket;

    public Bucket() {
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
