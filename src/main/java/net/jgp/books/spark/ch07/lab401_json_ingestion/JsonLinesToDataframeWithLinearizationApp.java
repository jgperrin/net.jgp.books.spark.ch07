package net.jgp.books.spark.ch07.lab401_json_ingestion;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * JSON Lines ingestion in a dataframe.
 * 
 * Note: This example is an extra, which is not described in chapter 7 of
 * the book: it illustrates how to access nested structure in the JSON
 * document directly. Linearization will be covered in chapter 12.
 * 
 * For more details about the JSON Lines format, see: http://jsonlines.org/.
 * 
 * @author jgp
 */
public class JsonLinesToDataframeWithLinearizationApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    JsonLinesToDataframeWithLinearizationApp app =
        new JsonLinesToDataframeWithLinearizationApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("JSON Lines to Dataframe")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read().format("json")
        .load("data/durham-nc-foreclosure-2006-2016.json");

    df = df.withColumn("year", col("fields.year"));
    df = df.withColumn(
        "coordinates",
        col("geometry.coordinates"));

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
  }
}
