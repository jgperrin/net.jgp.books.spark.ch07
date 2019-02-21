package net.jgp.books.spark.ch07.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Helper class to inspect a dataframe's schema. The printSchema() does not
 * display all the information.
 * 
 * @author jgp
 *
 */
public class SchemaInspector {

  public static void print(StructType schema) {
    print(null, schema);
  }

  public static void print(String label, Dataset<Row> df) {
    print(label, df.schema());
  }

  public static void print(String label, StructType schema) {
    if (label != null) {
      System.out.print(label);
    }
    System.out.println(schema.json());
  }

}
