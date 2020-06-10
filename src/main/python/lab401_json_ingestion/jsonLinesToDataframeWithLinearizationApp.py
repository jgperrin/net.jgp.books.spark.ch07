"""
  JSON Lines ingestion in a dataframe.

  Note: This example is an extra, which is not described in chapter 7 of
  the book: it illustrates how to access nested structure in the JSON
  document directly. Linearization will be covered in chapter 12.
  For more details about the JSON Lines format, see: http://jsonlines.org/.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/durham-nc-foreclosure-2006-2016.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("SON Lines to Dataframe") \
    .master("local[*]").getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("json") \
        .load(absolute_file_path)

df = df.withColumn("year", col("fields.year")) \
       .withColumn("coordinates", col("geometry.coordinates"))

# Shows at most 5 rows from the dataframe
df.show(5)
df.printSchema()

spark.stop()
