package net.jgp.books.sparkWithJava.ch07.lab_210.json_multiline_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Multiline ingestion JSON ingestion in a dataframe.
 * 
 * The data comes from The Bureau of Consular Affairs of the US Department of
 * State. You can access their open data portal at
 * https://cadatacatalog.state.gov/.
 * 
 * @author jgp
 */
public class MultilineJsonToDataframeWithCorruptRecordApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    MultilineJsonToDataframeWithCorruptRecordApp app =
        new MultilineJsonToDataframeWithCorruptRecordApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName(
            "Multiline JSON to Dataframe, without multiline option")
        .master("local")
        .getOrCreate();

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe,
    // without specifying the multiline option
    Dataset<Row> df = spark.read().format("json")
        .load("data/countrytravelinfo.json");

    // Shows at most 3 rows from the dataframe
    df.show(3);
    df.printSchema();
  }
}
