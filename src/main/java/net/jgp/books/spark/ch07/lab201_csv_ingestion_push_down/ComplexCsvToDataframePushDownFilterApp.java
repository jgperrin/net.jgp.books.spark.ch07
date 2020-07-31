package net.jgp.books.spark.ch07.lab201_csv_ingestion_push_down;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class ComplexCsvToDataframePushDownFilterApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ComplexCsvToDataframePushDownFilterApp app =
        new ComplexCsvToDataframePushDownFilterApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Complex CSV to Dataframe")
        .master("local")
        .getOrCreate();
    System.out.println("Using Apache Spark v" + spark.version());

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .option("multiline", true)
        .option("sep", ";")
        .option("quote", "*")
        .option("dateFormat", "MM/dd/yyyy")
        .option("inferSchema", true)
        .load("data/books.csv")
        .filter("authorId = 1");

    System.out.println("Excerpt of the dataframe content:");

    // Shows at most 7 rows from the dataframe, with columns as wide as 90
    // characters
    df.show(7, 70);
    System.out.println("Dataframe's schema:");
    df.printSchema();
  }
}
