package net.jgp.books.sparkWithJava.ch07.lab_110.csv_ingestion_schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * CSV ingestion in a dataframe with a Schema.
 * 
 * @author jperrin
 */
public class ComplexCsvToDataframeWithSchemaApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ComplexCsvToDataframeWithSchemaApp app =
        new ComplexCsvToDataframeWithSchemaApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .master("local")
        .getOrCreate();

    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "id",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "authordId",
            DataTypes.IntegerType,
            true),
        DataTypes.createStructField(
            "bookTitle",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "releaseDate",
            DataTypes.DateType,
            true),
        DataTypes.createStructField(
            "url",
            DataTypes.StringType,
            false) });

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .option("multiline", true)
        .option("sep", ";")
        .option("dateFormat", "M/d/y")
        .schema(schema)
        .load("data/books.csv");

    // Shows at most 15 rows from the dataframe
    df.show(15);
    df.printSchema();
  }
}
