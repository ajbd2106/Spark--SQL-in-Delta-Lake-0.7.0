package com.knoldus;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Scanner;

/**
 * DeltaLakeSQLOperations class has the methods that perform DDL, DML, and DQL on delta lake tables.
 */
public class DeltaLakeSQLOperations {

    /**
     * Creating Database.
     *
     * @param sparkSession Spark session.
     * @param dbName Database name.
     * @exception IOException IO Exception.
     */
    public void createDatabaseForDeltaTable(final SparkSession sparkSession,
                                            final String dbName) throws IOException {

        //Example: CREATE DATABASE IF NOT EXISTS sales
        sparkSession.sql(("CREATE DATABASE IF NOT EXISTS db_name").replaceAll("db_name", dbName));
        System.out.println("Database is successfully created.");
        System.out.println("Database " + dbName + " is successfully created.");
        userChoice(sparkSession);

    }

    /**
     * Creating Delta table.
     *
     * @param sparkSession Spark session.
     * @param dataBase Database name.
     * @param tableName Delta table name.
     * @param columnNames column name for delta table.
     * @exception IOException IO Exception.
     */
    public void createDeltaTable(final SparkSession sparkSession,
                                 final String tableName,
                                 final String columnNames,
                                 final String dataBase) throws IOException {

        //Example: CREATE TABLE sales.customer(c_id Long, c_name String, c_city String) USING DELTA
        sparkSession.sql(("CREATE TABLE table USING DELTA")
                .replaceAll("table", dataBase + "." + tableName + columnNames));
        System.out.println("Delta Table " + tableName + " is successfully created.");
        userChoice(sparkSession);

    }

    /**
     * Inserting data to Delta table.
     *
     * @param sparkSession Spark session.
     * @param dataBase Database name.
     * @param tableName Delta table name.
     * @param columnValues column values of delta table.
     * @exception IOException IO Exception.
     */
    public void insertIntoDeltaTable(final SparkSession sparkSession,
                                     final String tableName,
                                     final String columnValues,
                                     final String dataBase) throws IOException {

        //Example: INSERT INTO sales.customer VALUES(1, 'Jhon', 'New York')
        sparkSession.sql(("INSERT INTO TABLE VALUES")
                .replaceAll("TABLE", dataBase + "." + tableName)
                .replaceAll("VALUES", "VALUES" + columnValues));
        System.out.println("your Data  is successfully inserted into delta table " + tableName);
        userChoice(sparkSession);

    }

    /**
     * Getting history of Delta table.
     *
     * @param sparkSession Spark session.
     * @param dataBase Database name.
     * @param tableName Delta table name.
     * @exception IOException IO Exception.
     */
    public void describeHistory(final SparkSession sparkSession,
                                final String tableName,
                                final String dataBase) throws IOException {

        //Example: DESCRIBE HISTORY sales.customer
        sparkSession.sql(("DESCRIBE HISTORY table")
                .replaceAll("table", dataBase + "." + tableName)).show(true);
        userChoice(sparkSession);

    }

    /**
     * Reading Delta table by passing version number.
     *
     * @param sparkSession Spark session.
     * @param dataBase Database name.
     * @param tableName Delta table name.
     * @exception IOException IO Exception.
     */
    public void readDeltaTableWithVersion(final SparkSession sparkSession,
                                          final String tableName,
                                          final String dataBase,
                                          final Integer versionNumber) throws IOException {

        sparkSession.read().format("delta").option("versionAsOf", versionNumber)
                .table(dataBase + "." + tableName).show();
        userChoice(sparkSession);
    }

    /**
     * provided choice to user.
     */
    public void userChoice(final SparkSession sparkSession) throws IOException {

        System.out.println("Wants to executes more DDl operations press(y/n)? ");
        final Scanner myInput = new Scanner(System.in);
        final String yesOrNo = myInput.nextLine();
        if (yesOrNo.equals("y") || yesOrNo.equals("Y")) {
            DDLOperationSelection ddlOperationSelection = new DDLOperationSelection();
            ddlOperationSelection.selectDDLOperation(sparkSession);
        } else {
            System.exit(0);
        }
    }
}
