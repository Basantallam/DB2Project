import java.util.*;

public class PrecedenceStack {
    Stack<Object> stack ;
    Stack<Operation> stackO ; //Stack for Operations
    Table table;
    public PrecedenceStack(Table table){
        stack = new Stack<Object>();
        stackO = new Stack<Operation>();
        this.table = table;
    }
    public Vector<Hashtable> resolve(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        stack.push(sqlTerms[0]);
        stack.push(sqlTerms[1]);
        stackO.push(new Operation(arrayOperators[0]));
        for (int i = 2; i < arrayOperators.length; i++) {
            SQLTerm n = sqlTerms[i];
            Operation op = new Operation(arrayOperators[i - 1]);
            Operation top = stackO.peek();
            if (op.priority <= top.priority) { //top a7san
                stackO.pop();
                Object top1 = stack.pop();
                Object top2 = stack.pop();
                stack.push(applyOp(top1, top2, top.op));
            }
            stack.push(n);
            stackO.push(op);
        }
        while (stack.size() > 1) {
            Object a = stack.pop(); Object b = stack.pop();
            Operation o = stackO.pop();
            Vector<Hashtable> res = applyOp(a, b, o.op);
            stack.push(res);
        }
        return (Vector<Hashtable>) stack.pop(); //final result
    }

    public  Vector<Hashtable> applyOp(Object curr, Object next, String arrayOperator) throws DBAppException {
        switch (arrayOperator) {
            case ("AND"): return parentAND(curr, next);
            case ("OR"): //dayman "intersection" on sets
                if (curr instanceof SQLTerm) curr = table.resolveOneStatement((SQLTerm) curr);
                if (next instanceof SQLTerm) next = table.resolveOneStatement((SQLTerm) next);
                return ORing((Vector<Hashtable>) curr, (Vector<Hashtable>) next);
            case ("XOR"): //dayman operation on sets
                if (curr instanceof SQLTerm) curr = table.resolveOneStatement((SQLTerm) curr);
                if (next instanceof SQLTerm) next = table.resolveOneStatement((SQLTerm) next);
                return XORing((Vector<Hashtable>) curr, (Vector<Hashtable>) next);
            default:
                throw new DBAppException("Star operator must be one of AND, OR, XOR!");
        }
    }
    private Vector<Hashtable> parentAND(Object curr, Object next) throws DBAppException {
        //parent AND has 3 ANDing children "overloading"
        if (curr instanceof SQLTerm && next instanceof SQLTerm)
            return ANDing((SQLTerm)curr, (SQLTerm)next); //1st and child
        else {
            SQLTerm sqlTerm; Vector v;
            if (curr instanceof SQLTerm && next instanceof Vector){
                sqlTerm =(SQLTerm) curr; v=(Vector) next;
                return ANDing(sqlTerm,v);//2nd and child
            }
            else if(next instanceof SQLTerm && curr instanceof Vector){
                sqlTerm =(SQLTerm) next; v=(Vector) curr;
                return ANDing(sqlTerm,v);//2nd and child
                // theoretically 3omr ma da hayehsal bas just in case
            } else
                return ANDing((Vector<Hashtable>) curr, (Vector) next); // 3rd and child
                // theoretically 3omr ma da hayehsal bardo !
                // bas just in case
        }
    }
    public Vector<Hashtable> ANDing(SQLTerm term, Vector<Hashtable> v) throws DBAppException {
        for(Hashtable record:v)
            if(checkCond(record,term))
                v.add(record);
        return v;
    }

    private Vector<Hashtable> andSQLwithoutIndex(SQLTerm term1, SQLTerm term2, boolean clustering1, boolean clustering2) throws DBAppException {
        if(clustering1 || clustering2){
            Vector<Hashtable> res =clustering1? table.tableTraversal(term1): table.tableTraversal(term2);
            SQLTerm term = clustering1? term2 : term1;
            return ANDing(term,res);
        }
        else return table.LinearScan(term1, term2);
    }
    public static boolean checkCond(Hashtable rec, SQLTerm term) throws DBAppException {
        String col= term._strColumnName; Object value=term._objValue; String operator=term._strOperator;
        Object recVal = rec.get(col);
        switch (operator) {
            case (">"): return (Table.GenericCompare(recVal, value) > 0);
            case (">="):return (Table.GenericCompare(recVal, value) >= 0);
            case ("<"): return (Table.GenericCompare(recVal, value) <0);
            case ("<="):return (Table.GenericCompare(recVal, value) <= 0);
            case ("="): return (Table.GenericCompare(recVal, value) == 0);
            case ("!="):return (Table.GenericCompare(recVal, value) != 0);
            default:throw new DBAppException("Invalid Operator. Must be one of:   <,>,<=,>=,=,!=  ");
        }
    }
    Vector<Hashtable> ANDing(SQLTerm term1, SQLTerm term2) throws DBAppException {
        //2nd AND child
        Vector result = new Vector();
        Vector <String> terms=new Vector<String>();
        terms.add(term1._strColumnName);
        terms.add(term2._strColumnName);
        boolean clustering1=(term1._strColumnName.equals(table.clusteringCol));//todo indxPK else linear
        boolean clustering2=(term2._strColumnName.equals(table.clusteringCol));
        Index index = table.chooseIndex(terms); //todo wa7da tania 3shan n7ot priorities law not equal ma7otoosh equal a3la priority
        if (index != null) {
            if(term1._strOperator.equals("!=") && term2._strOperator.equals("!="))
                return andSQLwithoutIndex(term1, term2, clustering1, clustering2);
            return table.getTableRecords(index.andSelect(term1,term2),term1,term2);
        } else {
            return andSQLwithoutIndex(term1, term2, clustering1, clustering2);
        }
    }
    public Vector<Hashtable> ANDing(Vector<Hashtable> i1, Vector<Hashtable> i2){
        //hadkhol hena mn el XOR call bas
        Vector<Hashtable> result = new Vector<Hashtable>();
        HashSet<Hashtable> set1=new HashSet<Hashtable>(i1);
        HashSet<Hashtable> set2=new HashSet<Hashtable>(i2);

        for(Hashtable ht: set2)
            if(set1.contains(ht)) //set.contains = O(1)!!
                result.add(ht);
        return result;
    }
    public static Vector<Hashtable> ORing(Vector<Hashtable> i1, Vector<Hashtable> i2) { //Union Set Operation
        Set<Hashtable> s1 = new HashSet();
        s1.addAll(i1);
        Set<Hashtable> s2 = new HashSet();
        s2.addAll(i2);
        s1.addAll(s2); //set guarantees uniqueness

        Vector<Hashtable> res = new Vector<Hashtable>();
        res.addAll(s1);
        return res; //mmkn nkhali kolo y return iterator bas hanghayar 7abba fel code
    }
    public Vector<Hashtable> XORing(Vector<Hashtable> i1, Vector<Hashtable> i2) { //Set Operation
        Vector v1 = ORing(i1, i2); //UNION
        Vector v2 = ANDing(i1, i2); //INTERSECTION
        v1.removeAll(v2);
        return v1;
    }

    static class Operation {
        String op;
        int priority;

        public Operation(String o) throws DBAppException {
            op = o;
            if(o =="XOR") priority=1;
            else if (o =="OR") priority=2;
            else if(o=="AND")priority=3;
            else throw new DBAppException("invalid Operation");
        }

        public String toString() {
            return op;
        }
    }
}
