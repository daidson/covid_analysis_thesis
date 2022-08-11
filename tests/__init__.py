from pyspark.sql import SparkSession

SPARK = SparkSession.builder.appName("Ingestion Tests").getOrCreate()
