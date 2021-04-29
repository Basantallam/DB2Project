public class SQLTerm {
    String strTableName;
    String strColumnName;
    String strOperator;
    Object objValue;

    public SQLTerm(){

    }

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) throws DBAppException {
        this.strTableName=strTableName;
        this.strColumnName=strColumnName;
        if(strOperator.equals(">") || strOperator.equals(">=") ||
                strOperator.equals("<") || strOperator.equals("<=")||
                strOperator.equals("=") || strOperator.equals("!="))
            this.strOperator=strOperator;
        else
            throw new DBAppException("Invalid operator");
        this.objValue=objValue;

    }
}
