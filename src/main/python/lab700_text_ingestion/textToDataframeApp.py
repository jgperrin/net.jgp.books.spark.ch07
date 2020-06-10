"""
 Text ingestion in a dataframe.

 Source of file: Rome & Juliet (Shakespeare) -
 http://www.gutenberg.org/cache/epub/1777/pg1777.txt

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/romeo-juliet-pg1777.txt"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder \
    .appName("Text to Dataframe") \
    .master("local[*]") \
    .getOrCreate()

# Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
df = spark.read.format("text") \
        .load(absolute_file_path)

# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()

spark.stop()