package com.knoldus;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * DeltaLakeSQL is the main class where spark session is created.
 */
public final class DeltaLakeSQL {


    private static final Logger LOGGER = Logger.getLogger(DeltaLakeSQL.class);


    public static void main(String[] args) throws IOException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        LOGGER.info("Creating spark session with delta lake configuration");

        SparkSession sparkSession = SparkSession.builder().appName("DeltaLakeSQL ")
                .master("local")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        //instantiating DDLOperationSelection and call selectDDLOperation method to querying Delta lake DDL/DML
        DDLOperationSelection ddlOperationSelection = new DDLOperationSelection();
        ddlOperationSelection.selectDDLOperation(sparkSession);

        sparkSession.close();
    }
}
