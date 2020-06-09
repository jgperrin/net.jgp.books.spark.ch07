package net.jgp.books.spark.ch07.lab600_xml_ingestion

import org.apache.spark.sql.SparkSession

/**
 * XML ingestion to a dataframe.
 *
 * Source of file: NASA patents dataset -
 * https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm
 *
 * @author rambabu.posa
 */
object XmlToDataframeScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("XML to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .load("data/nasa-patents.xml")

    // Shows at most 5 rows from the dataframe
    df.show(5)
    df.printSchema

    spark.stop
  }

}
