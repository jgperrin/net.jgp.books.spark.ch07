package net.jgp.books.sparkWithJava.ch07.lab_300.xml_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * XML ingestion to a dataframe.
 * 
 * Source of file:
 * NASA patents dataset - https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm
 * 
 * @author jgp
 */
public class XmlToDataframeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    XmlToDataframeApp app = new XmlToDataframeApp();
    app.start();
  }

  /**
   * The processing code.//#C
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("XML to Dataframe")
        .master("local")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("XML")
        .option("rowTag", "row")
        .load("data/nasa-patents.xml");

    // Shows at most 5 rows from the dataframe
    df.show(5);
    df.printSchema();
  }
}
