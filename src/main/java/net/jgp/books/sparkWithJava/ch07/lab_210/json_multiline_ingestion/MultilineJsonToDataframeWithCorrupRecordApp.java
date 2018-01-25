package net.jgp.books.sparkWithJava.ch07.lab_210.json_multiline_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * JSON Lines ingestion in a dataframe.
 * 
 * For more details about the JSON Lines format, see: http://jsonlines.org/.
 * 
 * @author jgp
 */
public class MultilineJsonToDataframeWithCorrupRecordApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MultilineJsonToDataframeWithCorrupRecordApp app =
        new MultilineJsonToDataframeWithCorrupRecordApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Multiline JSON to Dataset")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("json")
        .load("data/countrytravelinfo.json");

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
  }
}
