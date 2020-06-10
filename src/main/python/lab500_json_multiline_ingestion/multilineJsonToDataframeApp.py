"""
 Multiline ingestion JSON ingestion in a dataframe.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/countrytravelinfo.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Multiline JSON to Dataframe") \
    .master("local[*]").getOrCreate()

# Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

# Shows at most 3 rows from the dataframe
df.show(3)
df.printSchema()

spark.stop()