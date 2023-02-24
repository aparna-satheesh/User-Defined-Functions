# Databricks notebook source
# MAGIC %md
# MAGIC ### What are UDFs?
# MAGIC 
# MAGIC - UDFs or User Defined Functions are a way to extend the functionality of Spark SQL by allowing users to define their own functions that can be used in SQL expressions.
# MAGIC 
# MAGIC - UDFs are defined using programming languages like Python, Scala, or Java, and can be registered with Spark to make them available for use in SQL expressions. Once registered, UDFs can be used in the same way as built-in functions to transform or manipulate the data in a Spark DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why are UDFs used?
# MAGIC 
# MAGIC - UDFs allow users to write custom code to perform specific data processing tasks, which can be more efficient and flexible than using the built-in functions provided by Spark SQL.
# MAGIC 
# MAGIC - UDFs allow users to express logic in familiar languages, reducing the human cost associated with refactoring code.For ad hoc queries, manual data cleansing, exploratory data analysis, and most operations on small or medium-sized datasets, latency overhead costs associated with UDFs are unlikely to outweigh costs associated with refactoring code.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to create UDFs in Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Python Function
# MAGIC 
# MAGIC The first step in creating a UDF is creating a Python function. The below snippet is a function to derive air time of flight in hours from an existing column

# COMMAND ----------

def calculate_duration_hrs(air_time):
    return round(air_time / 60,2)

# COMMAND ----------

# MAGIC %run /Users/aparna.menon@diggibyte.com/UDF_run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting Python function to PySpark UDF

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Using select()

# COMMAND ----------

air_time_udf = udf(calculate_duration_hrs, FloatType())

# COMMAND ----------


df = flights.select('*',air_time_udf(flights.air_time).alias("air_time_hrs"))
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using withColumn()

# COMMAND ----------

def long_flight(distance):
    return 1 if distance > 1000 else 0

# COMMAND ----------

udf_long_flight = udf(long_flight, IntegerType())

# COMMAND ----------

df1 = flights.withColumn("long_flight", udf_long_flight("distance"))
df1.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Registering predefined function
# MAGIC 
# MAGIC To register a custom function as a UDF, you need to use the udf function provided by PySpark. This function takes your custom function as an argument, and returns a new function that can be registered with Spark using the **spark.udf.register** method.
# MAGIC 
# MAGIC >Syntax
# MAGIC ````python
# MAGIC def func_name(param1,param2,..):
# MAGIC   #function logic
# MAGIC 
# MAGIC   #register the udf  
# MAGIC spark.udf.register("func_name", func_name)
# MAGIC 
# MAGIC   # create a temporary view for the flights DataFrame
# MAGIC df.createOrReplaceTempView("table_name")
# MAGIC 
# MAGIC   # use the UDF in a Spark SQL expression to concatenate origin and dest columns
# MAGIC concat_flights = spark.sql("SELECT column1,column2,func_name(param1, ) as alias_name FROM table_name")
# MAGIC 
# MAGIC ````

# COMMAND ----------

def concat_origin_dest(origin, dest):
    return f"{origin}-{dest}"

# COMMAND ----------

# register the UDF with Spark
spark.udf.register("concat_origin_dest", concat_origin_dest)

# COMMAND ----------

# create a temporary view for the flights DataFrame
flights.createOrReplaceTempView("flights")

# use the UDF in a Spark SQL expression to concatenate origin and dest columns
concat_flights = spark.sql("SELECT flight,distance,air_time,concat_origin_dest(origin, dest) as origin_dest FROM flights")

# display the result
concat_flights.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using @udf Annotations
# MAGIC 
# MAGIC UDF can be created in one step without the explicitly assigning it using udf function using the @udf annotation.
# MAGIC > Syntax <br>
# MAGIC ```python
# MAGIC @udf(returnType=returnType())
# MAGIC def function_Name(): 
# MAGIC   #function logic
# MAGIC   return return_value
# MAGIC ```

# COMMAND ----------

flights.groupBy("carrier").count().show()

# COMMAND ----------

carrier_dict = {
  'ZW': 'Air Wisconsin',
  'AS': 'Alaska Airlines',
  'G4': 'Allegiant Air LLC',
  'AA': 'American Airlines',
  'C5': 'Champlain Air',
  'CP': 'Compass Airlines',
  'DL': 'Delta Air Lines, Inc.',
  'EM': 'Empire Airline',
  '9E': 'Endeavor Air',
  'MQ': 'Envoy Air',
  'EV': 'ExpressJet Airlines',
  'F9': 'Frontier Airlines, Inc.',
  'G7': 'GoJet Airlines',
  'HA': 'Hawaiian Airlines Inc.',
  'QX': 'Horizon Air',
  'B6': 'Jetblue Airways Corporation',
  'OH': 'Jetstream Intl',
  'YV': 'Mesa Airlines, Inc.',
  'KS': 'Penair',
  'PT': 'Piedmont Airlines',
  'YX': 'Republic Airlines',
  'OO': 'Skywest Airlines',
  'WN': 'Southwest Airlines',
  'NK': 'Spirit Airlines, Inc.',
  'AX': 'Trans State',
  'UA': 'United Airlines, Inc.'
}


# COMMAND ----------

@udf(returnType=StringType())
def convert_abbreviation(value):
    if value in carrier_dict:
        return carrier_dict[value]
    else:
        return value

# COMMAND ----------

df2 = flights.withColumn("Carrier_Name", convert_abbreviation(flights["carrier"]))
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Evaluation order and null checking
# MAGIC 
# MAGIC Spark SQL (including SQL and the DataFrame and Dataset API) does not guarantee the order of evaluation of subexpressions. In particular, the inputs of an operator or function are not necessarily evaluated left-to-right or in any other fixed order. 
# MAGIC 
# MAGIC If a UDF relies on short-circuiting semantics in SQL for null checking, there’s no guarantee that the null check will happen before invoking the UDF.
# MAGIC 
# MAGIC ````python
# MAGIC spark.udf.register("strlen", lambda s: len(s), "int")
# MAGIC spark.sql("select s from test1 where s is not null and strlen(s) > 1") # no guarantee
# MAGIC ````
# MAGIC This WHERE clause does not guarantee the strlen UDF to be invoked after filtering out nulls.
# MAGIC 
# MAGIC To perform proper null checking, we recommend that you do either of the following:
# MAGIC 
# MAGIC - Make the UDF itself null-aware and do null checking inside the UDF itself
# MAGIC 
# MAGIC - Use IF or CASE WHEN expressions to do the null check and invoke the UDF in a conditional branch
# MAGIC 
# MAGIC ````python
# MAGIC spark.udf.register("strlen_nullsafe", lambda s: len(s) if not s is None else -1, "int")
# MAGIC spark.sql("select s from test1 where s is not null and strlen_nullsafe(s) > 1") // ok
# MAGIC spark.sql("select s from test1 where if(s is not null, strlen(s), null) > 1")   // ok
# MAGIC ````

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Concerns with UDF

# COMMAND ----------

# MAGIC %md
# MAGIC UDFs can have performance concerns when working with large datasets in distributed systems like PySpark. Here are some of the performance concerns with UDFs:
# MAGIC 
# MAGIC - **Serialization and Deserialization**: Since UDFs are Python functions, they need to be serialized and deserialized to be sent to the worker nodes for execution. This process can be slow, especially for large UDFs or when working with a lot of data.
# MAGIC 
# MAGIC - **Garbage Collection**: UDFs create a lot of garbage objects, which can cause performance issues due to increased memory usage and garbage collection overhead.
# MAGIC 
# MAGIC - **Network Overhead**: UDFs need to send data over the network to worker nodes for processing. This can cause performance issues if the network is slow or if there is a lot of data to be processed.
# MAGIC 
# MAGIC - **Single-Threaded Execution**: By default, UDFs execute in a single thread on each worker node, which can cause performance issues when processing large datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC To optimize performance with UDFs, you can take the following steps:
# MAGIC 
# MAGIC - Use built-in PySpark functions whenever possible, as they are optimized for distributed processing.
# MAGIC 
# MAGIC - Use the PySpark DataFrame API instead of RDDs, as it provides optimizations for performance.
# MAGIC 
# MAGIC - Avoid using UDFs for complex processing and instead use PySpark's higher-level APIs, such as Window, groupBy, join, etc.
# MAGIC 
# MAGIC - Minimize the amount of data passed to and from UDFs, as serialization and deserialization can be slow and memory-intensive.
# MAGIC 
# MAGIC - Consider using Pandas UDFs (also known as Vectorized UDFs), which can provide performance benefits for certain types of operations. However, they have their own limitations, such as being memory-intensive and not suitable for all types of operations.

# COMMAND ----------

# MAGIC %md 
# MAGIC ####References
# MAGIC - https://docs.databricks.com/udf/python.html
# MAGIC - https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/#pyspark-udf
