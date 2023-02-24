# Databricks notebook source
#importing DataTypes and SQL fucntions 
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,BooleanType
from pyspark.sql.functions import udf, when, col,lit

# COMMAND ----------

flight_schema=StructType([
        StructField('year',IntegerType(), True ),
         StructField('month', IntegerType(), True),
         StructField('day', IntegerType(), True),
         StructField('dep_time', IntegerType(), True),
        StructField('dep_delay', IntegerType(), True),
        StructField('arr_time', IntegerType(), True),
        StructField('arr_delay', IntegerType(), True),
        StructField('carrier', StringType(), True),
        StructField('tailnum', StringType(), True),
        StructField('flight', IntegerType(), True),
        StructField('origin', StringType(), True),
        StructField('dest', StringType(), True),
        StructField('air_time', IntegerType(), True),
        StructField('distance', IntegerType(), True),
        StructField('hour', IntegerType(), True),
        StructField('minute', IntegerType(), True)
         ])

# COMMAND ----------

flights=spark.read.format("csv").option("header","true").schema(flight_schema).load("dbfs:/FileStore/flights_small.csv")
flights.show(2)
