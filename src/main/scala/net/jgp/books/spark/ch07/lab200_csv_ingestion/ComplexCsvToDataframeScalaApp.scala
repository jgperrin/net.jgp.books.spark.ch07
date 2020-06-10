package net.jgp.books.spark.ch07.lab200_csv_ingestion

import org.apache.spark.sql.SparkSession

/**
 * CSV ingestion in a dataframe.
 *
 * @author rambabu.posa
 */
object ComplexCsvToDataframeScalaApp {

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
    val spark = SparkSession.builder
      .appName("Complex CSV to Dataframe")
      .master("local[*]")
      .getOrCreate

    println("Using Apache Spark v" + spark.version)

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("quote", "*")
      .option("dateFormat", "MM/dd/yyyy")
      .option("inferSchema", true)
      .load("data/books.csv")

    println("Excerpt of the dataframe content:")

    // Shows at most 7 rows from the dataframe, with columns as wide as 90
    // characters
    df.show(7, 70)
    println("Dataframe's schema:")
    df.printSchema()

    spark.stop
  }

}
