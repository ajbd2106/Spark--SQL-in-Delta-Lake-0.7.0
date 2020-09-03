package com.knoldus;

import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * DDLOperationSelection gives user flexibility to select a DDL, DML, and DQL operation.
 */
public class DDLOperationSelection {

    /**
     * Number of triangles passes through each vertex.
     *
     * @param sparkSession Spark session.
     * @exception IOException IO Exception
     */
    public void selectDDLOperation(final SparkSession sparkSession) throws IOException {

        System.out.println("--------------------------------------------------");
        System.out.println("Press 1 to create database for Delta Tables.");
        System.out.println("Press 2 to create a Delta Table in your Database.");
        System.out.println("Press 3 to Insert data into your Delta Table.");
        System.out.println("Press 4 to read your Delta Table.");
        System.out.println("Press 5 to perform Update/Delete/Merge operation on your Delta table.");
        System.out.println("Press 6 to get the latest history of your delta table.");
        System.out.println("Press 7 to read yor Delta table of by passing a version of table.");
        System.out.println("Press 8 to drop a Delta table you want");
        System.out.println("---------------------------------------------------");


        System.out.println("Enter a Number for which DDL operation you want to execute");
        Scanner myInput = new Scanner( System.in );

        int ddlNumber = myInput.nextInt();
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in));

        //Instantiating DeltaLakeSQLOperations class that contains DDLOperations
        final DeltaLakeSQLOperations deltaLakeSQLOperations =
                new DeltaLakeSQLOperations();

        switch(ddlNumber) {

            case 1: {
                System.out.println("Please enter a name of your Database to create");
                String databaseName =  myInput.next();
                deltaLakeSQLOperations.createDatabaseForDeltaTable(sparkSession, databaseName);

            }

            case 2: {
                System.out.println("Please enter a Database name in which you want to create your delta table");
                final String dataBaseName = myInput.next();
                System.out.println("Please enter a Delta table name to create");
                final String tableName = myInput.next();
                System.out.println("Please enter a comma separated column Names inside parenthesis like" +
                        " (c_id Long, c_name String, c_city String)");

                String columnNames = reader.readLine();
                deltaLakeSQLOperations
                        .createDeltaTable(sparkSession,tableName, columnNames, dataBaseName);
            }

            case 3: {
                System.out.println("Please enter a Database name in which your delta table is reside");
                final String dataBaseName = myInput.next();
                System.out.println("Please enter a table name to insert data");
                final String tableName = myInput.next();
                System.out.println("Please enter a comma separated column values inside parenthesis to " +
                        "insert into table like (1, 'Jhon', 'New York')");
                final String columnValues = reader.readLine();;
                deltaLakeSQLOperations
                        .insertIntoDeltaTable(sparkSession, tableName, columnValues, dataBaseName);
            }

            case 4: {
                System.out.println("Please enter a select Query to read your Delta table");
                System.out.println("example: 'SELECT * FROM TABLE'");
                sparkSession.sql(reader.readLine()).show();
                deltaLakeSQLOperations.userChoice(sparkSession);
            }

            case 5: {
                System.out.println("Please enter a query to perform Update/Delete/Merge operation on your Delta table.");
                System.out.println("Delete example: DELETE FROM sales.customer WHERE c_name == 'Jhon' " +
                        "sales is a database and customer is a table ");
                System.out.println("Update example: UPDATE sales.customer SET c_city = 'California'  WHERE c_id == 1");
                System.out.println("Merge example: MERGE INTO sales.customer USING new_customer\n" +
                        "                                ON sales.customer.c_id = sales.new_customer.c_id\n" +
                        "                                WHEN MATCHED THEN\n" +
                        "                                 UPDATE SET sales.customer.c_city = sales.new_customer.c_city\n" +
                        "                                WHEN NOT MATCHED THEN INSERT *");
                sparkSession.sql(reader.readLine());
                System.out.println("your operation performed successfully");
                deltaLakeSQLOperations.userChoice(sparkSession);
            }

            case 6: {
                System.out.println("Please enter a Database name in which your delta table is reside");
                final String dataBaseName = myInput.next();
                System.out.println("Please enter a table name to get its latest history");
                final String tableName = myInput.next();
                deltaLakeSQLOperations.describeHistory(sparkSession, tableName, dataBaseName);
            }

            case 7: {
                System.out.println("Please enter a Database name in which your delta table is reside");
                final String dataBaseName = myInput.next();
                System.out.println("Please enter a table name to read it");
                final String tableName = myInput.next();
                System.out.println("Please enter a version number like 1, 2 etc. of your delta table you want to read it");
                final Integer versionNumber = myInput.nextInt();
                deltaLakeSQLOperations.readDeltaTableWithVersion(sparkSession, tableName,dataBaseName, versionNumber);
            }

            case 8: {
                System.out.println("Please enter a Database name in which your delta table is reside");
                final String dataBaseNmae = myInput.next();
                System.out.println("Please enter your delta table name to drop it");
                final String tableName = myInput.next();

                sparkSession.sql(("DROP TABLE IF EXISTS table").replaceAll("table", dataBaseNmae + "." + tableName));
                System.out.println("Delta Table " + tableName + " is successfully Dropped.");
                deltaLakeSQLOperations.userChoice(sparkSession);
            }

            default:
                System.out.println("No such operation available for this number, please enter only provided numbers");
                break;
        }

    }

}
