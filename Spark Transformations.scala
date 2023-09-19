// Databricks notebook source
// MAGIC %md
// MAGIC Repartition, partitionBy, bucketBy
// MAGIC ==================================

// COMMAND ----------

val directory = "dbfs:/FileStore/tables/week12/"

// COMMAND ----------

import org.apache.spark.sql.functions._
//dbutils.fs.ls("dbfs:/FileStore/tables/week12")

// COMMAND ----------

// MAGIC %md
// MAGIC Simple Repartition
// MAGIC ===========================
// MAGIC repartition involves full shuffle
// MAGIC repartition is blind repartition, we dont know what goes to what file
// MAGIC but it controls the number of files generated with file size
// MAGIC
// MAGIC alternate way is doing partitioning and bucketing
// MAGIC
// MAGIC third way is using sorting -- sortBy
// MAGIC whenver we do bucketing and sorting, it can give a lot of optimization
// MAGIC
// MAGIC note: number of output files is equal to the number of partitions in your dataframe.
// MAGIC benefit of repartition: it can help you increase the parallelism
// MAGIC
// MAGIC with normal repartition, partition pruning is not possible

// COMMAND ----------

val ordersDf = spark.read 
.format("csv")
.option("header", true)
.option("inferschema", true)
.option("path",directory+"orders_week12.csv")
.load()
				
print("ordersDf has "+ordersDf.rdd.getNumPartitions)

val ordersRep = ordersDf.repartition(4)

print("ordersRep has "+ordersRep.rdd.getNumPartitions)


// COMMAND ----------

ordersDf.count()

// COMMAND ----------

ordersRep.write
.mode(SaveMode.Overwrite)
.option("path",directory+"orders_week12output.csv")
.save()

//saveModes
//=========
//1. append
//2. overwrite
//3. errorIfExists
//4. ignore

// COMMAND ----------

//you can see 4 parts 
dbutils.fs.ls("dbfs:/FileStore/tables/week12/orders_week12output.csv/")

// COMMAND ----------

// MAGIC %md
// MAGIC partitionBy
// MAGIC ====================
// MAGIC
// MAGIC This is equal to paritioning in hive, it provides partition pruning. By using this approach can help to skip some of the partitions, as you can see below, you can go the order_status = cancelled and will get records of cancelled orders, without going through other partitions.

// COMMAND ----------

ordersRep.write
.format("csv")
.partitionBy("order_status")
.mode(SaveMode.Overwrite)
.option("path",directory+"orders_week12output1")
.save()

// COMMAND ----------

//you can the partitions based on order_status
dbutils.fs.ls("dbfs:/FileStore/tables/week12/orders_week12output1")

// COMMAND ----------

ordersRep.write
.format("avro")
.partitionBy("order_status")
.mode(SaveMode.Overwrite)
.option("maxRecordsPerFile",2000)
.option("path",directory+"orders_week12output1")
.save()

// COMMAND ----------

val path="dbfs:/FileStore/tables/week12/orders_week12output1/order_status=PROCESSING"
val filelist=dbutils.fs.ls(path)
val df = filelist.toDF() 

// COMMAND ----------

//df.createOrReplaceTempView("order_status_size")
display(spark.sql("select *  from order_status_size"))

// COMMAND ----------

// MAGIC %md
// MAGIC table & View
// MAGIC ============

// COMMAND ----------

//this orders table is distributed across machine
ordersDf.createOrReplaceTempView("orders")

// COMMAND ----------

//sparksql performance is equivalent to dataframe
//if your problem can't be solved by sparksql then go with dataframe or dataset
//if you need more flexibility than dataframe or dataset then go with RDD
val resultsDF = spark.sql("select order_status, count(*) as num_of_orders from orders group by order_status order by 2 desc")

// COMMAND ----------

resultsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC table
// MAGIC =====
// MAGIC
// MAGIC
// MAGIC 1. data : spark warehouse : spark.sql.warehouse.dir
// MAGIC 2. metadata : catalog metastore : in memory (not persistent)
// MAGIC
// MAGIC Here, since metadata is not persistent, therefore, we can also use hive metastore to handle spark metadata.

// COMMAND ----------

//it will create in default database
ordersDf.write
.format("csv")
.mode(SaveMode.Overwrite)
.saveAsTable("orders1")

// COMMAND ----------

dbutils.fs.ls("dbfs:/")

// COMMAND ----------

spark.catalog.listTables("default").show()

// COMMAND ----------

//creating my own database
spark.sql("create database if not exists retails")

// COMMAND ----------

ordersDf.write
.format("csv")
.mode(SaveMode.Overwrite)
.saveAsTable("retails.orders")

// COMMAND ----------

spark.catalog.listTables("retails").show()

// COMMAND ----------

// MAGIC %md
// MAGIC bucketBy
// MAGIC ========

// COMMAND ----------

//the order_customer_id has high cardinality, so we can do bucketBy on order_customer_id attribute
ordersDf.write
.format("csv")
.mode(SaveMode.Overwrite)
.bucketBy(4, "order_customer_id")
.sortBy("order_customer_id")
.saveAsTable("retails.orders")

// COMMAND ----------

// MAGIC %md
// MAGIC Transformations
// MAGIC ===============
// MAGIC
// MAGIC
// MAGIC 1. Low Level Transformations
// MAGIC ============================
// MAGIC
// MAGIC a. map
// MAGIC b. filter
// MAGIC c. groupByKey
// MAGIC
// MAGIC we can perform low level transformations using raw rdds. some of it are even possible with dataframe and datasets.
// MAGIC In rdd, there is no concept of column, each row is of string type.
// MAGIC
// MAGIC 2. High Level Transformations
// MAGIC =============================
// MAGIC
// MAGIC a. select
// MAGIC b. where
// MAGIC c. groupBy
// MAGIC
// MAGIC Note: High level transformations are supported only by Dataframes and Datasets.

// COMMAND ----------

val lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/week12/orders_week12.csv")

// COMMAND ----------

val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r

// COMMAND ----------

case class Orders(order_id: Int, order_customer_id: Int, order_status: String)

def parser(line: String) = {
  line match {
    case myregex(order_id,order_customer_id,order_status) => Orders(order_id.toInt,order_customer_id.toInt, order_status)
  }
}

// COMMAND ----------

val rdd = lines.map(parser)

// COMMAND ----------

import spark.implicits._
val ordersDS = lines.map(parser).toDS().cache()

// COMMAND ----------

ordersDS.printSchema()

// COMMAND ----------

ordersDS.select("order_id").show()

// COMMAND ----------

ordersDS.groupBy("order_status").count().show()

// COMMAND ----------

// MAGIC %md
// MAGIC How to refer columns in a Dataframe or Dataset?
// MAGIC ===============================================
// MAGIC
// MAGIC 1. Column String
// MAGIC 2. Column Object

// COMMAND ----------

//Column String 
ordersDf.select("order_id","order_status").show()

// COMMAND ----------

//Column Object -- $ is scala specific
import spark.implicits._
ordersDf.select(column("order_id"),col("order_date"),$"order_customer_id",'order_status').show()

// COMMAND ----------

//converting column expression into column object
ordersDf.select(column("order_id"),expr("concat(ordedr_status,'_STATUS')")).show(false)
//another better way of doing it
ordersDf.selectExpr("order_id","concat(ordedr_status,'_STATUS')").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC User Defined Function (UDF)
// MAGIC ===========================

// COMMAND ----------

val df = spark.read 
.format("csv")
.option("inferschema", true)
.option("path",directory+"week12-1.dataset1")
.load()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

//Defining my column names
val df1 = df.toDF("name","age","city")
df1.printSchema()

// COMMAND ----------

df1.show()

// COMMAND ----------

//converting df1 dataframe to dataset
case class Person(name:String, age:Int, city:String)

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
val df1: Dataset[Row] = df.toDF("name","age","city")

// COMMAND ----------

//convert dataframe to dataset
//dataset gives you compile time safety and dataframe doesn't. that's the only difference
val ds1 = df1.as[Person]

// COMMAND ----------

//convert dataset to dataframe
val df2 = ds1.toDF()

// COMMAND ----------

//column object expression udf
//creating a function

def ageCheck(age:Int):String = {
  if (age>18) "Y" else "N"
}

//register the function
//basically we register the function with the driver 
//and the driver will serialize the function and will send it to each executor
val parseAge = udf(ageCheck(_:Int):String)

// COMMAND ----------

import org.apache.spark.sql.functions._
val df2 = df1.withColumn("adult", parseAge(column("age")))

// COMMAND ----------

//you will not see the function in catalog because only sql/string expression udf are shown in catalog
spark.catalog.listFunctions().filter(x => x.name == "parseAge").show()

// COMMAND ----------

//sql/string expression udf 
spark.udf.register("parseAge", ageCheck(_:Int):String)

val df2 = df1.withColumn("adult", expr("parseAge(age)"))

// COMMAND ----------

//you will now see the function in catalog
//if it is in catalog, we can fire normal sql queries

spark.catalog.listFunctions().filter(x => x.name == "parseAge").show()

// COMMAND ----------

df1.createOrReplaceTempView("peopletable")
spark.sql("select name, city, parseAge(age) as adult from peopletable").show()

// COMMAND ----------

//Exercise
//create a scala list
//from list, create a dataframe
//dataframe columns : orderid, orderdate, customerid, status
//once you created dataframe, convert the date field from the given format to epoch timestamp (unix_timestamp - number of seconds after 1st jan 1970)
//create your own unique id which will be your 5th column and make sure it has unique id's in any order
//drop duplicates based on column orderdate, customerid
//drop the orderid column
//sort it based on orderdate

// COMMAND ----------

//create a list

val myList = List(
  (1, "2013-07-25", 11599, "CLOSED"),
  (2, "2014-07-25", 256, "PENDING_PAYMENT"),
  (3, "2013-07-25", 11599, "COMPLETE"),
  (4, "2019-07-25", 8827, "CLOSED")
  )

// COMMAND ----------

//list to dataframe
import spark.implicits._
val ordersDf = spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status")

// COMMAND ----------

ordersDf.printSchema()

// COMMAND ----------

import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.monotonically_increasing_id

// COMMAND ----------

val newDf = ordersDf
.withColumn("orderdate", unix_timestamp(col("orderdate").cast(DateType)))
.withColumn("newid", monotonically_increasing_id)
.dropDuplicates("orderdate","customerid")
.drop("orderid")
.sort("orderdate")

// COMMAND ----------

newDf.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Aggregate Transformations
// MAGIC =========================
// MAGIC
// MAGIC
// MAGIC 1. Simple Aggregations
// MAGIC 2. Grouping Aggregates
// MAGIC 3. Window Aggregates

// COMMAND ----------

//load the file and create a dataframe, do it using standard dataframe reader api
//calculate totalNumberOfRows, totalQuantity, avgUnitPrice, numberOfUniqueInvoices using column object expression
//doing the above same using string expression
//doing the above same using spark sql

// COMMAND ----------

val invoiceDf = spark.read 
.format("csv")
.option("header", true)
.option("inferschema", true)
.option("path",directory+"order_data_week12.csv")
.load()

// COMMAND ----------

invoiceDf.show()

// COMMAND ----------

import org.apache.spark.sql.functions._
invoiceDf.select(count("*").as("RowCount"), sum("Quantity").as("totalQuantity"), avg("UnitPrice").as("AvgPrice"), 
                 countDistinct("InvoiceNo").as("CountDistinct")).show()

// COMMAND ----------

invoiceDf.selectExpr("count(*) as RowCount", "sum(Quantity) as totalQuantity", "avg(UnitPrice) as AvgPrice", "count(Distinct(InvoiceNo)) as CountDistinct").show()

// COMMAND ----------

invoiceDf.createOrReplaceTempView("sales")
spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show()

// COMMAND ----------

//group the data based on country and invoice number 
//calculate the total quantity of each group, sum of invoice value (sum (quantity * unitprice))
//do it using column object expression first
//then do it using string expression
//then do it using spark sql expression

// COMMAND ----------

//column object expression
val summaryDf = invoiceDf.groupBy("Country","InvoiceNo").agg(sum("Quantity").as("TotalQuantity"),
                                            sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
                                            )

summaryDf.show()

// COMMAND ----------

//string based expression
val summaryDf1 = invoiceDf.groupBy("Country","InvoiceNo").agg(expr("sum(Quantity) as TotalQuantity"),
                                            expr("sum(Quantity * UnitPrice) as InvoiceValue"))

summaryDf1.show()

// COMMAND ----------

//spark sql
invoiceDf.createOrReplaceTempView("sales")
val summaryDf2 = spark.sql("""select country, invoiceno, sum(quantity) as totalquantity, sum(quantity*unitprice) as invoicevalue from sales group by country, invoiceno""")
summaryDf2.show()

// COMMAND ----------

//Window Aggregations 
//partition column - country
//order column - weeknum
//window size - from 1st row to the current row

// COMMAND ----------

val windowDf = spark.read 
.format("csv")
.option("inferschema", true)
.option("path",directory+"windowdata_week12-1.csv")
.load()

// COMMAND ----------

val windowDf1 = windowDf.toDF("country","weeknum","numinvoices","totalquantity","invoicevalue")

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val myWindow = Window.partitionBy("country")
.orderBy("weeknum")
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

val myDf = windowDf1.withColumn("RunningTotal", sum("invoicevalue").over(myWindow))

// COMMAND ----------

myDf.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Joins on DataFrame
// MAGIC ==================
// MAGIC 1. Simple Join (Shuffle sort merge join)
// MAGIC 2. Broadcast Join (does not require a shuffle)

// COMMAND ----------

// MAGIC %md
// MAGIC When to go with broadcast join or simple join?
// MAGIC
// MAGIC when we are joining two large dataframe then we have to go with the simple join and shuffle will be required.
// MAGIC when we have one large dataframe and the other dataframe is smaller, we can then go with the broadcast join.

// COMMAND ----------

///FileStore/tables/week12/customers_week12-1.csv
val ordersDf = spark.read 
.format("csv")
.option("header", true)
.option("inferschema", true)
.option("path",directory+"orders_week12.csv")
.load()


///FileStore/tables/week12/customers_week12-1.csv
val customersDf = spark.read 
.format("csv")
.option("header", true)
.option("inferschema", true)
.option("path",directory+"customers_week12.csv")
.load()

// COMMAND ----------

// orders dataframe inner join customers dataframe
//likewise you can perform left, right, full joins
val joinedDf = ordersDf.join(customersDf, ordersDf.col("order_customer_id") === customersDf.col("customer_id"),"inner")

joinedDf.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Internals of normal join operation
// MAGIC ==================================
// MAGIC 1. it requires shuffling because the keys can be in different executors
// MAGIC 2. once the mapping of key is done (let say executor2), it will write the output into the exchange
// MAGIC 3. from this exchange (its like a buffer in the executor), spark framework can read it and do the shuffle.
// MAGIC 4. now all the records with the same key will go the same reduce exchange (let say executor3) (view the DAG in SPARK UI)
// MAGIC 5. the simple join involves shuffle sort merge join (you can see in Spark UI under SQL tab)
// MAGIC 6. Also, when you use inner join, the system tries to optimize it to use broadcast join (you can view it in Spark UI sql tab)
// MAGIC

// COMMAND ----------

//orders and customers number of partitions
print("ordersDf has "+ordersDf.rdd.getNumPartitions)
print("\ncustomersDf has "+customersDf.rdd.getNumPartitions)

// COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/week12")

// COMMAND ----------

// MAGIC %md
// MAGIC biglog.txt file
// MAGIC ================
// MAGIC > Using this file, do a grouping based on logging level and month,
// MAGIC > there are five different loggin levels and 12 months,
// MAGIC > output should contain 60 rows

// COMMAND ----------

import spark.implicits._
val mylist = List("WARN,2016-12-31 04:19:32",
                 "FATAL,2016-12-31 03:22:34",
                 "WARN,2016-12-31 03:21:21",
                 "INFO,2015-4-21 14:32:21",
                 "FATAL,2015-4-21 19:23:20")

// COMMAND ----------

val rdd1 = spark.sparkContext.parallelize(mylist)

// COMMAND ----------

case class Logging(level:String, datetime:String)

// COMMAND ----------

def mapper(line:String): Logging = {
  val fields = line.split(',')
  val logging:Logging = Logging(fields(0), fields(1))
  return logging
}

// COMMAND ----------

val rdd2 = rdd1.map(mapper)

// COMMAND ----------

val df1 = rdd2.toDF()

// COMMAND ----------

df1.show()

// COMMAND ----------

//same result using spark-sql
df1.createOrReplaceTempView("logging_table")
spark.sql("select * from logging_table").show()

// COMMAND ----------

//now group the results based on level and the datetime should be in list [date1, date2....]
spark.sql("select level, collect_list(datetime) from logging_table group by level order by level").show()

// COMMAND ----------

//check how many logging levels are there for each type
spark.sql("select level, count(datetime) from logging_table group by level order by level").show()

// COMMAND ----------

//get the month from the datetime
spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table").show()

// COMMAND ----------

//capture it inside a dataframe
val df2 = spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table") 

// COMMAND ----------

val biglogDf = spark.read 
.format("csv")
.option("header", true)
.option("path",directory+"biglog_week12.txt")
.load()

// COMMAND ----------

biglogDf.show()

// COMMAND ----------

biglogDf.createOrReplaceTempView("new_logging_table")
spark.sql("select * from new_logging_table").show()

// COMMAND ----------

spark.sql("""select level, date_format(datetime, 'MMMM') as month, count(1) as total 
from new_logging_table group by level, month""").show()

// COMMAND ----------

spark.sql("""select level, date_format(datetime, 'MMMM') as month, count(1) as total 
from new_logging_table group by level, month order by month""").show()

// COMMAND ----------

// the above query sorted it in alphbetical order but i need it in month wise starting from January
spark.sql("""select level, date_format(datetime, 'MMMM') as month, date_format(datetime, 'M') as monthnum, count(1) as total 
from new_logging_table group by level, month, monthnum order by monthnum""").show()

// COMMAND ----------

val results1 = spark.sql("""select level, date_format(datetime, 'MMMM') as month, date_format(datetime, 'M') as monthnum, count(1) as total from new_logging_table group by level, month, monthnum order by monthnum""")

// COMMAND ----------

val results2 = results1.drop("monthnum")

// COMMAND ----------

spark.sql("select * from new_logging_table").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Exercise
// MAGIC =========
// MAGIC Given 2 Datasets employee.json and dept.jsonWe need to calculate the count of employees against each department. 
// MAGIC Use Structured API’s.
// MAGIC
// MAGIC Sample output:
// MAGIC
// MAGIC 0. depName,deptid,empcount
// MAGIC 1. IT,11,1
// MAGIC 2. HR,21,1
// MAGIC 3. Marketing,31,1
// MAGIC 4. Fin,41,2
// MAGIC 5. Admin,51,0
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC employee json data
// MAGIC ==================
// MAGIC
// MAGIC {"id":"1201","empname":"satish","age":"25","salary":5000,"address":[{"city":"Pune","state":"Maharashtra"}],"deptid":11}
// MAGIC {"id" : "1202", "empname": "krishna", "age":"28", "salary":6000,"address":[{"city":"Pune","state":"Maharashtra"}],"deptid":21}
// MAGIC {"id" : "1203", "empname": "amith", "age":"39", "salary":7000,"address":[{"city":"Pune","state":"Maharashtra"}],"deptid":31}
// MAGIC {"id" : "1204", "empname": "javed", "age":"23", "salary":8000,"address":[{"city":"Pune","state":"Maharashtra"}],"deptid":41}
// MAGIC {"id" : "1205", "empname": "prudvi", "age":"23", "salary":9000,"address":[{"city":"Pune","state":"Maharashtra"}],"deptid":41}

// COMMAND ----------

// MAGIC %md
// MAGIC dept json data
// MAGIC ==============
// MAGIC
// MAGIC {"deptid":11,"deptName":"IT"}
// MAGIC {"deptid":21,"deptName":"HR"}
// MAGIC {"deptid":31,"deptName":"Marketing"}
// MAGIC {"deptid":41,"deptName":"Fin"}
// MAGIC {"deptid":51,"deptName":"Admin"}
