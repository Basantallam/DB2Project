import java.io.Serializable;

public class Index implements Serializable {
    String[] columnNames;
    Object[] buckets;

    public Index(String[] columnNames) {
        this.columnNames = columnNames;
        int n = columnNames.length;
        buckets= new Bucket[10];//
        String str= "[10]";
        for (int i = 1; i < n; i++) {
            str+="[10]";
        }
//        buckets= str.toArray();
        this.fill();
    }

    private void fill() {
    }

    private class Bucket implements Serializable{
    }
}
