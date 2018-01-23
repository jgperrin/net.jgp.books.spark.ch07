package net.jgp.books.sparkWithJava.ch07.lab_100.csv_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jperrin
 */
public class ComplexCsvToDataframeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
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

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .option("multiline", true)
        .option("sep", ";")
        .option("dateFormat", "m/d/y")
        .option("inferSchema", true)
        .load("data/books.csv");

    // Shows at most 15 rows from the dataframe
    df.show(15);
    df.printSchema();
  }
}
