import java.util.*;

public class PrecedenceStack {
    Stack<Object> stack ;
    Stack<DBApp.Operation> stackO ;
    Table table;
    public PrecedenceStack(Table table){
        stack = new Stack<Object>();
        stackO = new Stack<DBApp.Operation>();
        this.table = table;
    }
    public Vector<Hashtable> resolve(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        stack.push(sqlTerms[0]);
        stack.push(sqlTerms[1]);
        stackO.push(new DBApp.Operation(arrayOperators[0]));
        for (int i = 2; i < arrayOperators.length; i++) {
            SQLTerm n = sqlTerms[i];
            DBApp.Operation op = new DBApp.Operation(arrayOperators[i - 1]);
            DBApp.Operation top = stackO.peek();
            if (op.priority <= top.priority) { //top a7san
                stackO.pop();
                Object topn = stack.pop();
                Object topn2 = stack.pop();
                stack.push(applyOp(topn, topn2, top.op));
                stack.push(n);
                stackO.push(op);
            } else {
                stack.push(n);
                stackO.push(op);
            }
        }
        while (stack.size() > 1) {
            Object a = stack.pop();
            Object b = stack.pop();
            DBApp.Operation o = stackO.pop();
            Vector<Hashtable> res = applyOp(a, b, o.op);
            stack.push(res);
        }
        return (Vector<Hashtable>) stack.pop();
    }
    public  Vector<Hashtable> applyOp(Object curr, Object next, String arrayOperator) throws DBAppException {
        switch (arrayOperator) {
            case ("AND"): return ANDing(curr, next);
            case ("OR"):
                if (curr instanceof SQLTerm) curr = table.resolveOneStatement((SQLTerm) curr);
                if (next instanceof SQLTerm) next = table.resolveOneStatement((SQLTerm) next);
                return ORing((Vector<Hashtable>) curr, (Vector<Hashtable>) next);
            case ("XOR"):
                if (curr instanceof SQLTerm) curr = table.resolveOneStatement((SQLTerm) curr);
                if (next instanceof SQLTerm) next = table.resolveOneStatement((SQLTerm) next);
                return XORing((Vector<Hashtable>) curr, (Vector<Hashtable>) next);
            default:
                throw new DBAppException("Star operator must be one of AND, OR, XOR!");
        }
    }
    private Vector<Hashtable> ANDing(Object curr, Object next) throws DBAppException {
        //parent AND
        if (curr instanceof SQLTerm && next instanceof SQLTerm)
            return ANDingI((SQLTerm)curr, (SQLTerm)next); //1st and child
        else {
            SQLTerm sqlTerm;
            Vector v;
            if (curr instanceof SQLTerm && next instanceof Vector){
                sqlTerm =(SQLTerm) curr; v=(Vector) next;
                //2nd and child
                return null;
            }
            else if(next instanceof SQLTerm && curr instanceof Vector){
                sqlTerm =(SQLTerm) next; v=(Vector) curr;
                //2nd and child
                return null;
                // theoretically 3omr ma da hayehsal bas just in case
            }
            else
                // theoretically 3omr ma da hayehsal bardo !
                // bas just in case
            return ANDing((Vector<Hashtable>) curr, (Vector) next);
                // 3rd and child

        }
    }
    public Vector<Hashtable> ANDing(SQLTerm term, Vector<Hashtable> v) throws DBAppException {
        for(Hashtable record:v){
            if(checkCond(record,term)){
                v.add(record);
            }
        }
        return v;
    }

    private Vector<Hashtable> andSQLwithoutIndex(SQLTerm term1, SQLTerm term2, boolean clustering1, boolean clustering2) throws DBAppException {
        Vector result = new Vector();
        if(clustering1 || clustering2){
            Vector<Hashtable> res =clustering1? table.tableTraversal(term1): table.tableTraversal(term2);
            for(Hashtable record:res){
                if(checkCond(record, clustering1?term2:term1)){
                    result.add(record);
                }
            }
            return result;
        }
        else{
            return table.LinearScan(term1, term2);
        }
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
    Vector<Hashtable> ANDingI(SQLTerm term1, SQLTerm term2) throws DBAppException {
        //2nd AND child
        Vector result = new Vector();
        Vector <String> terms=new Vector<String>();
        terms.add(term1._strColumnName);
        terms.add(term2._strColumnName);
        boolean clustering1=(term1._strColumnName.equals(table.clusteringCol));//todo indxPK else linear
        boolean clustering2=(term2._strColumnName.equals(table.clusteringCol));
        Index index = table.chooseIndexAnd(terms); //todo wa7da tania 3shan n7ot priorities law not equal ma7otoosh equal a3la priority
        if (index != null) {
            if(term1._strOperator.equals("!=")&& term2._strOperator.equals("!="))
                return andSQLwithoutIndex(term1, term2, clustering1, clustering2);
            return table.getTableRecords(index.andSelect(term1,term2),term1,term2);
            //todo mesh 3arfaaaaa
        } else {
            return andSQLwithoutIndex(term1, term2, clustering1, clustering2);
        }
    }
    public Vector<Hashtable> ANDing(Vector<Hashtable> i1, Vector<Hashtable> i2){
        Vector<Hashtable> result = new Vector<Hashtable>();
        for(Hashtable ht1: i1){
            if(i2.contains(ht1)){
                result.add(ht1);
            }
        }
        return result;
    }
    public static Vector<Hashtable> ORing(Vector<Hashtable> i1, Vector<Hashtable> i2) { //Union Set Operation
//        Set<Hashtable> s1 = new HashSet();
//        Set<Hashtable> s2 = new HashSet(i2);
//        s1.addAll(s2);
        i1.addAll(i2);
//        Vector<Hashtable> res = new Vector<Hashtable>();
//        res.addAll(s1);
        return i1;
    }
    public Vector<Hashtable> XORing(Vector<Hashtable> i1, Vector<Hashtable> i2) { //Set Operation
        Vector v2 = ANDing(i1, i2);
        Vector v1 = ORing(i1, i2);
        Vector<Hashtable> res = new Vector<Hashtable>();
        Collections.sort(v1);
        Collections.sort(v2);
        Iterator it1 = v1.iterator();
        Iterator it2 = v2.iterator();
        Object o1 = it1.next();
        Object o2 = it2.next();
        while (it1.hasNext()) {
            if (Table.GenericCompare(o1, o2) == 0) {
                o1 = it1.next();
                if (it2.hasNext())
                    o2 = it2.next();
            } else if (Table.GenericCompare(o1, o2) < 0) {
                res.add((Hashtable) o1);
                o1 = it1.next();
            } else if (Table.GenericCompare(((Hashtable)o1).get(table.clusteringCol), ((Hashtable)o2).get(table.clusteringCol)) > 0) {
                if (it2.hasNext())
                    o2 = it2.next();
                else
                    break;
            }
        }
        while (it1.hasNext()) {
            res.add((Hashtable) o1);
            o1 = it1.next();
        }
        res.add((Hashtable)o1);
        return res;
    }

    static class Operation {
        String op;
        int priority;

        public Operation(String o) throws DBAppException {
            op = o; int p=0;
            if(o =="XOR") p=1;
            else if (o =="OR") p=2;
            else if(o=="AND")p=3;
            else throw new DBAppException("invalid Operation");
            priority = p;
        }

        public String toString() {
            return op;
        }
    }
}
