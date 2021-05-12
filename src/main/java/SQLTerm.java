public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm(){

    }

    public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) throws DBAppException {
        this._strTableName =strTableName;
        this._strColumnName =strColumnName;
        if(strOperator.equals(">") || strOperator.equals(">=") ||
                strOperator.equals("<") || strOperator.equals("<=")||
                strOperator.equals("=") || strOperator.equals("!="))
            this._strOperator =strOperator;
        else
            throw new DBAppException("Invalid operator");
        this._objValue =objValue;

    }
}
