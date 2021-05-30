import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;



@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Milestone2Tests {

    @Test
    @Order(1)
    public void testTableCreation() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        createStudentTable(dbApp);
        createCoursesTable(dbApp);
        createTranscriptsTable(dbApp);
        createPCsTable(dbApp);

        dbApp = null;
    }

    @Test
    @Order(2)
    public void testRecordInsertions() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        int limit = 500;

        insertStudentRecords(dbApp, limit);
        dbApp = null;
    }

    @Test
    public void testCreateDateIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "courses";
        String[] index = {"date_added"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateIntegerIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "pcs";
        String[] index = {"pc_id"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateDoubleIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "transcripts";
        String[] index = {"gpa"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateStringIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"id"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateStringDoubleIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"id", "gpa"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateStringDateIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "transcripts";
        String[] index = {"course_name", "date_passed"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testCreateDoubleDateIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"gpa", "dob"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }


    @Test
    public void testCreateStringStringIndex() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        String table = "students";
        String[] index = {"first_name", "last_name"};
        dbApp.createIndex(table, index);
        dbApp = null;
    }

    @Test
    public void testSelectEmptyStudents() throws Exception{
        // Should return an empty iterator with no errors thrown

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue ="John";

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = new Double(0.7);

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "AND";

        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
    }

    @Test
    public void testSelectActualStudentOR() throws Exception{
        // Should return a non-empty iterator with no errors thrown

        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = 0;
        int finalLine = 1;
        Hashtable<String, Object> row = new Hashtable();


        while ((record = studentsTable.readLine()) != null && c <= finalLine) {
            if (c == finalLine) {
                String[] fields = record.split(",");
                row.put("id", fields[0]);
                row.put("first_name", fields[1]);
                row.put("last_name", fields[2]);

                int year = Integer.parseInt(fields[3].trim().substring(0, 4));
                int month = Integer.parseInt(fields[3].trim().substring(5, 7));
                int day = Integer.parseInt(fields[3].trim().substring(8));


                Date dob = new Date(year - 1900, month - 1, day);
                row.put("dob", dob);

                double gpa = Double.parseDouble(fields[4].trim());

                row.put("gpa", gpa);

            }
            c++;
        }
        studentsTable.close();


        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue =row.get("first_name");

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "<=";
        arrSQLTerms[1]._objValue = row.get("gpa");

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
    }

    @Test
    public void testSelectActualStudentAND() throws Exception{
        // Should return a non-empty iterator with no errors thrown

        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = 0;
        int finalLine = 1;
        Hashtable<String, Object> row = new Hashtable();


        while ((record = studentsTable.readLine()) != null && c <= finalLine) {
            if (c == finalLine) {
                String[] fields = record.split(",");
                row.put("id", fields[0]);
                row.put("first_name", fields[1]);
                row.put("last_name", fields[2]);

                int year = Integer.parseInt(fields[3].trim().substring(0, 4));
                int month = Integer.parseInt(fields[3].trim().substring(5, 7));
                int day = Integer.parseInt(fields[3].trim().substring(8));


                Date dob = new Date(year - 1900, month - 1, day);
                row.put("dob", dob);

                double gpa = Double.parseDouble(fields[4].trim());

                row.put("gpa", gpa);

            }
            c++;
        }
        studentsTable.close();


        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strTableName = "students";
        arrSQLTerms[0]._strColumnName= "first_name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue =row.get("first_name");

        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strTableName = "students";
        arrSQLTerms[1]._strColumnName= "gpa";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = row.get("gpa");

        String[]strarrOperators = new String[1];
        strarrOperators[0] = "AND";
// select * from Student where name = “John Noor” or gpa = 1.5;
        DBApp dbApp = new DBApp();
        dbApp.init();
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
    }


    private void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }


    private void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    private void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }


    private void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }

    private void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
}
