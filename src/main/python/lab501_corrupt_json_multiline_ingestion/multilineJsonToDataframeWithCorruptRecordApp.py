"""
   Failing multiline ingestion JSON ingestion in a dataframe.

   This example illustrates what happens when you forget the multiline
   option and try to ingest a multiline JSON file.

   Output is:

   <pre>
   +--------------------+
   |     _corrupt_record|
   +--------------------+
   |                 [ {|
   |       "tag" : "A1",|
   |  "geopoliticalar...|
   +--------------------+
   only showing top 3 rows

   root
   |-- _corrupt_record: string (nullable = true)
  * </pre>

   The data comes from The Bureau of Consular Affairs of the US Department
   of State. You can access their open data portal at
   https://cadatacatalog.state.gov/.

   @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/countrytravelinfo.json"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Multiline JSON to Dataframe, without multiline option") \
    .master("local[*]").getOrCreate()

# Reads a JSON, called countrytravelinfo.json, stores it in a
# dataframe,
# without specifying the multiline option
df = spark.read.format("json") \
          .load(absolute_file_path)

# Shows at most 3 rows from the dataframe
df.show(3)
df.printSchema()

spark.stop()