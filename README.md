# Database Engine Simulator Project

We created a project in Java that simulates the behaviour of a Database Engine. We applied the concepts of database query optimization and indexing which we previously learned theoretically. 
We focused on Grid Indexing as our indexing technique in this project. Given any query, our program chooses either to use one of the existing grid indexes or queries on the table directly depending on what would be more time efficient. 

## Classes:

We created the database engine in an object oriented design consisting of the following classes:

- **DBApp Class** is the main class which contains all the high-order methods in our program

- **Table Class** is a class which represents a table in the database

- **Page Class** is a class which represents a "memory page" in a table

- **Index Class** is a class which represents a grid index in the database

- **Bucket Class** is a class which represents a "memory bucket" in a grid index 

- **SQLTerm Class** is a class which represents an SQL query

DBApp class is our main class which contains all the high-order methods in our program.

The main features of the project are executed by one of these methods:

1. createTable
2. insertIntoTable
3. updateTable
4. deleteFromTable
5. createIndex
6. insertIntoTable
7. selectFromTable
8. deleteFromTable
9. updateTable

<img width="337" alt="image" src="https://user-images.githubusercontent.com/30272808/186530334-7ac8374e-84b1-49bd-9f2f-bdbd54e2f1d1.png">


Here is an example code that creates a table, creates an index, does few inserts, and a select:

```java
String strTableName = "Student";
DBApp dbApp = new DBApp( );
Hashtable htblColNameType = new Hashtable( );
htblColNameType.put("id", "java.lang.Integer");
htblColNameType.put("name", "java.lang.String");
htblColNameType.put("gpa", "java.lang.double");

dbApp.createTable( strTableName, "id", htblColNameType /* in addition to min max hashtables” );
dbApp.createIndex( strTableName, new String[] {"gpa"} );

Hashtable htblColNameValue = new Hashtable( );

htblColNameValue.put("id", new Integer( 2343432 ));
htblColNameValue.put("name", new String("Ahmed Noor" ) );
htblColNameValue.put("gpa", new Double( 0.95 ) );

dbApp.insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 453455 ));
htblColNameValue.put("name", new String("Ahmed Noor" ) );
htblColNameValue.put("gpa", new Double( 0.95 ) );

dbApp.insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 5674567 ));
htblColNameValue.put("name", new String("Dalia Noor" ) );
htblColNameValue.put("gpa", new Double( 1.25 ) );

dbApp.insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 23498 ));
htblColNameValue.put("name", new String("John Noor" ) );
htblColNameValue.put("gpa", new Double( 1.5 ) );

dbApp.insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 78452 ));
htblColNameValue.put("name", new String("Zaky Noor" ) );
htblColNameValue.put("gpa", new Double( 0.88 ) );
dbApp.insertIntoTable( strTableName , htblColNameValue );

SQLTerm[] arrSQLTerms;
arrSQLTerms = new SQLTerm[2];
arrSQLTerms[0]._strTableName = "Student";
arrSQLTerms[0]._strColumnName= "name";
arrSQLTerms[0]._strOperator = "=";
arrSQLTerms[0]._objValue = "John Noor";
arrSQLTerms[1]._strTableName = "Student";
arrSQLTerms[1]._strColumnName= "gpa";
arrSQLTerms[1]._strOperator = "=";
arrSQLTerms[1]._objValue = new Double( 1.5 );
String[]strarrOperators = new String[1];
strarrOperators[0] = "OR";

// select * from Student where name = “John Noor” or gpa = 1.5;
Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
```

## Some Key Features in Our Project:

1) Each table/relation is stored as binary pages on disk and not in a single file

2) Table is sorted on key

3) Each table and page is loaded only upon need and not always kept in memory. Page is loaded into memory and removed from memory once not needed.

4) A page is stored as a vector of objects

5) Meta data file is used to learn about types of columns in a table with every select/insert/delete/update

6) Page maximum row count is loaded from metadata file (N value)

7) A column can have any of data types

8) You can do a select query without having any index created

9) You can do a select query with the existence of an index that could be used to reduce search space

10) You can insert into a table without having any index created

11) You can insert with the existence of an index that could be used to reduce search space

12) Delete a record without having any index created

13) Delete a record with the existence of an index that could be used to reduce search space

14) Update a record without having any index created is working fine

15) Update a record with the existence of an index that could be used to reduce search space

16) Create an index for a given column whether it's a key column or otherwise.

17) Save and load index from disk

18) Insert and delete from index correctly
