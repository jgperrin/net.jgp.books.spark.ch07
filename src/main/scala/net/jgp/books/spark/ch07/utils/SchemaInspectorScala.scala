package net.jgp.books.spark.ch07.utils

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType

/**
 * Helper class to inspect a dataframe's schema. The printSchema() does not
 * display all the information.
 *
 * @author rambabu.posa
 *
 */
object SchemaInspectorScala {

  def print(schema: StructType): Unit = print(None, schema)

  def print(label: String, df: Dataset[Row]): Unit = print(Some(label), df.schema)

  def print(label: Option[String]=None, schema: StructType): Unit = {
    if (label != null) println(label)
    println(schema.json)
  }

}
