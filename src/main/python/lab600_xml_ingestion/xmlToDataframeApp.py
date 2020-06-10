"""
   XML ingestion to a dataframe.

   Source of file: NASA patents dataset -
   https://data.nasa.gov/Raw-Data/NASA-Patents/gquh-watm

   @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/nasa-patents.xml"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("XML to Dataframe") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
    .getOrCreate()

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("xml") \
        .option("rowTag", "row") \
        .load(absolute_file_path)

# Shows at most 5 rows from the dataframe
df.show(5)
df.printSchema()

spark.stop()