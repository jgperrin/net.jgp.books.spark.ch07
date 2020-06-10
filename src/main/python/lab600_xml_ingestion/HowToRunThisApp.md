We use the following code to create spark session:

```
spark = SparkSession.builder.appName("XML to Dataframe") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
    .getOrCreate()
```

Here we use "spark.jars.packages" to load additional packages.

Unfortuntely, this code snippet does not work as expected
```
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") 
```

So we can use "--packages" command-line option to run this application

First, clone the project and cd to net.jgp.books.spark.ch07/src/main/python/lab600_xml_ingestion

```
spark-submit --packages com.databricks:spark-xml_2.12:0.9.0 xmlToDataframeApp.py
```

We can also use "--conf" command-line option to run this application as shown below:

```
spark-submit --conf "spark.jars.packages=com.databricks:spark-xml_2.12:0.9.0"  xmlToDataframeApp.py
```