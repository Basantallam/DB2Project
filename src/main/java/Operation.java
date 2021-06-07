import org.junit.jupiter.api.parallel.Resources;

import java.util.Iterator;
import java.util.Stack;
import java.util.Vector;

public class Operation { //CLASS FOR TESTING MESH AKTAR
    static int grid[][][]={{{1,2,3,},{5,6,7,},{9,10,11,}},
                         {{13,14,15},{17,18,19},{21,22,23}},
                         {{25,26,27},{29,30,31},{33,34,35}}};
    String op;
    int priority;
    public Operation(String o,int p){
        op=o;
        priority=p;
    }
    public String toString(){
        return op;
    }


    public static void main(String args[]){
//        Stack<Object> stack=new Stack<Object>();
//        Stack<Operation> stackO=new Stack<Operation>();
//        Vector v=new Vector<Object>();
//        Vector<Operation> vO=new Vector<Operation>();
//        v.add(10);
//        v.add(3);
//        v.add(2);
//        v.add(5);
//        v.add(2);
//        v.add(4);
//        v.add(3);
//
//        int res=0;
////       10+(3*2*5)+(2*4*3)
//        vO.add(new Operation("+",1));
//        vO.add(new Operation("*",2));
//        vO.add(new Operation("*",2));
//        vO.add(new Operation("+",1));
//        vO.add(new Operation("*",2));
//        vO.add(new Operation("*",2));
//        stack.push(v.get(0));
//        stack.push(v.get(1));
//        stackO.push(vO.get(0));
//       for(int i=2;i<v.size();i++) {
//           System.out.println(stack);
//           System.out.println(stackO);
//           int n = (Integer) v.get(i);
//           Operation op = vO.get(i - 1);
//           Operation top = stackO.peek();
//           if (op.priority <= top.priority) { //top a7san
//               stackO.pop();
//               int topn = (Integer) stack.pop();
//               int topn2 = (Integer) stack.pop();
//               if (top.op == "*") {
//                   stack.push((Integer) (topn * topn2));
//               } else {
//                   stack.push((Integer) (topn + topn2));
//               }
//               stack.push((Integer) n);
//               stackO.push(op);
//           }
//            else{
//                stack.push(n);
//                stackO.push(op);
//            }
//       }
//        while(stack.size()>1){
//            System.out.println(stack);
//            System.out.println(stackO);
//            int a=(Integer)stack.pop();
//            int b= (Integer) stack.pop();
//            Operation o = stackO.pop();
//            if(o.op=="*"){
//                res=((Integer)(a*b));
//                stack.push(res);
//            }
//            else{
//                res=((Integer)(a+b));
//                stack.push(res);
//            }
//        }
//        System.out.println(stack);
//        System.out.println(stackO);
//        System.out.println(stack.pop());
//        int[] curr= {0,0,0};
//        int[] lim= {2,1,0};
//
//        Vector<Integer> v = new Vector<>();
//        loop(curr,lim,0,v);
//        System.out.println(v.size());
//        System.out.println(v);
        Iterator it=returnsIt();
        while(it.hasNext())
            System.out.println(it.next());
    }

    public static Iterator returnsIt(){
        Vector v =new Vector<Integer>();
        v.add(1);
        v.add(2);
        v.add(3);
        v.add(4);
        v.add(5);
        return v.iterator();
    }
    public static void loop(int[] curr,int[] limits,int depth, Vector<Integer> accumulated){
        if(depth==limits.length){
            System.out.println(curr.toString());
            accumulated.add(getCell(curr));
            return;
        }
        for(int i=0;i<=limits[depth];i++){
            int[] newCurr = curr.clone();
            newCurr[depth]=i;
            loop(newCurr,limits,depth+1,accumulated);
        }
    }
    public static void printindex2DI(Index index ){
        String tablename = index.tableName;
        Vector columnNames = index.columnNames;
        int size =0;
        for (int i = 0; i< 10 ; i++) {
            for (int j = 0; j < 10; j++) {
                System.out.println("grid["+i+"]["+j+"]");
                Vector<Index.BucketInfo> cell =((Vector<Index.BucketInfo>)((Object[][])index.grid)[i][j]);
                for (int l = 0; l <cell.size() ; l++) {
                    Index.BucketInfo bi = cell.get(l);
                    Bucket b = (Bucket) DBApp.deserialize(tablename+"_"+columnNames+"_"+bi.id);
                    System.out.println("Bucket "+b.id);
                    for (int k = 0; k <b.records.size() ; k++) {
                        System.out.println(b.records.get(k));
                    size++;
                    }
                    DBApp.serialize(tablename+"_"+columnNames+"_"+bi.id,b);
                }
            }
        }
        System.out.println("Size ="+size);
    }
    public static void printindex2DJ(Index index ){
        String tablename = index.tableName;
        Vector columnNames = index.columnNames;
        int size=0;
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i< 10 ; i++) {
                System.out.println("grid["+i+"]["+j+"]");
                Vector<Index.BucketInfo> cell =((Vector<Index.BucketInfo>)((Object[][])index.grid)[i][j]);
                for (int l = 0; l <cell.size() ; l++) {
                    Index.BucketInfo bi = cell.get(l);
                    Bucket b = (Bucket) DBApp.deserialize(tablename+"_"+columnNames+"_"+bi.id);
                    System.out.println("Bucket "+b.id);
                    for (int k = 0; k <b.records.size() ; k++) {
                        System.out.println(b.records.get(k));
                    size++;
                    }
                    DBApp.serialize(tablename+"_"+columnNames+"_"+bi.id,b);
                }
            }
        }
        System.out.println("Size ="+size);
    }

    private static int getCell(int[] curr) {
        return grid[curr[0]][curr[1]][curr[2]];
    }
}
