"""
 CSV ingestion in a dataframe.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Complex CSV to Dataframe") \
    .master("local[*]").getOrCreate()

print("Using Apache Spark v{}".format(spark.version))

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", True) \
        .option("sep", ";") \
        .option("quote", "*") \
        .option("dateFormat", "MM/dd/yyyy") \
        .option("inferSchema", True) \
        .load(absolute_file_path)

print("Excerpt of the dataframe content:")

# Shows at most 7 rows from the dataframe, with columns as wide as 90
# characters
df.show(7, 70)
print("Dataframe's schema:")
df.printSchema()

spark.stop