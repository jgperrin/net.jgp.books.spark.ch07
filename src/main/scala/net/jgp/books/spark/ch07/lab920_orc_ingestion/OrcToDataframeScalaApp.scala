package net.jgp.books.spark.ch07.lab920_orc_ingestion

import org.apache.spark.sql.SparkSession

/**
 * ORC ingestion in a dataframe.
 *
 * Source of file: Apache ORC project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author rambabu.posa
 */
object OrcToDataframeScalaApp {

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
      .appName("ORC to Dataframe")
      .config("spark.sql.orc.impl", "native")
      .master("local[*]")
      .getOrCreate

    // Reads an ORC file, stores it in a dataframe
    val df = spark.read
      .format("orc")
      .load("data/demo-11-zlib.orc")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()

    println(s"The dataframe has ${df.count} rows.")

    spark.stop
  }

}
