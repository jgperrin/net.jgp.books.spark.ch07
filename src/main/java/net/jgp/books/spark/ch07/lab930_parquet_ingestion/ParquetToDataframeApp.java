package net.jgp.books.spark.ch07.lab930_parquet_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet ingestion in a dataframe.
 * 
 * Source of file: Apache Parquet project -
 * https://github.com/apache/parquet-testing
 * 
 * @author jgp
 */
public class ParquetToDataframeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    ParquetToDataframeApp app = new ParquetToDataframeApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Parquet to Dataframe")
        .master("local")
        .getOrCreate();

    // Reads a Parquet file, stores it in a dataframe
    Dataset<Row> df = spark.read()
        .format("parquet")
        .load("data/alltypes_plain.parquet");

    // Shows at most 10 rows from the dataframe
    df.show(10);
    df.printSchema();
    System.out.println("The dataframe has " + df.count() + " rows.");
  }
}
