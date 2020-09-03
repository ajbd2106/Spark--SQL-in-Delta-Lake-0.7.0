# SQL-DDLs-DMLs-DQL-in-Delta-Lake-0.7.0

The project consists of 3 Java Classes:

1. DeltaLakeSQL - This is the main class where spark session is created.
2. DeltaLakeSQLOperations - This class has the methods that perform DDL, DML, and DQL on delta lake tables.
3. DDLOperationSelection - This gives user flexibility to select a DDL, DML, and DQL operation.


##Table of contents

1.  [Getting Started] (#Getting-Started) 
2.  [How to Run] (#How to Run)
3.  [How to Use] (#How to use)
    
##Getting Started
####Minimum requirements

To run the SDK you will need **Java 1.8+, Scala 2.12 spark-core 3.0.0, Delta Lake 0.7.0 **.
Installation

The way to use this project is to clone it from github and build it using Mave.
##How to Run

    mvn clean install
    mvn exec:java

##How to Use

    Enter the option number for the the DDLs/DML/DQL you want to execute
    Output should be visible on the console.

