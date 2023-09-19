// Databricks notebook source
// MAGIC %md
// MAGIC Performance Optimization
// MAGIC ========================
// MAGIC
// MAGIC Focus on two main areas:
// MAGIC 1. Cluster Configuration level - resource level optimization
// MAGIC 2. Application code level - how we write the code.
// MAGIC > For example - use Partitioning, Bucketing, cache & persist, avoid or minimize shuffling of data, join optimization, use optimized file formats, use reduceByKey instead of groupByKey

// COMMAND ----------

// MAGIC %md
// MAGIC Resources
// MAGIC =========
// MAGIC Memory (RAM), CPU cores (Compute)
// MAGIC
// MAGIC We should make sure our job should get the right amount of resources.
// MAGIC
// MAGIC For Example: 
// MAGIC > We have 10 node cluster (10 worker nodes). Each node has 16 CPU cores (1 core is alloted to background processes) and each node has 64 GB Ram
// MAGIC
// MAGIC Executor (is like a container of resources)
// MAGIC > 1 node can hold more than one executor (multiple container - CPU cores + Memory (RAM))
// MAGIC
// MAGIC There are 2 strategies when creating executors.
// MAGIC 1. Thin Executor
// MAGIC >  the intention is to create more executors with each executor holding minimum possible resources. Total is 16 executors, so each executor  will hold 1 core, 4 GB Ram. Here, we won't be able to do multithreading as each executor holds only one core. Also, when you do broadcast, here a lot of copies of broadcast variable are required because each executor should receive its own copy. These two are drawbacks of Thin Executor.
// MAGIC
// MAGIC 2. Fat Executor
// MAGIC > the intention is to give maximum resources to each executor. So here we can create each executor to hold 16 cores, 64 GB ram. The main drawback of this executor is if it holds more than 5 CPU cores then the HDFS throughput suffers. And if the executor holds very huge amount of memory then the garbage collection takes a lot of time. 

// COMMAND ----------

// MAGIC %md
// MAGIC Production Approach
// MAGIC ===================
// MAGIC
// MAGIC Scenario - We have 10 nodes where each node has
// MAGIC > 16 cores, 64 GB RAM
// MAGIC
// MAGIC Overhead
// MAGIC >1 core is given for other background activities, and 1 GB RAM is given for Operating System.
// MAGIC
// MAGIC Now each node we have 15 cores, 63 GB RAM, and we want multithreading within a executor and we should not let HDFS throughput to suffer.
// MAGIC
// MAGIC To make this happen, we chose 5 CPU cores in each executor.Now we have 3 executors running on each worker node. 
// MAGIC Each executor will contain 5 CPU cores and 21 GB RAM (Total RAM divided by No of executors).
// MAGIC
// MAGIC Note: Out of 21 GB RAM, some of it will go as part of overhead (Off heap memory). Max - (384MB, 7% of executor memory) ~ 1.5 GB
// MAGIC
// MAGIC So, we have
// MAGIC > 10 node * 3 executor = 30 executors across the cluster
// MAGIC
// MAGIC > Each executor is holding 5 CPU cores and approx 19 GB RAM (21GB - 1.5GB)
// MAGIC
// MAGIC > 1 executor out of these 30 will be given for YARN Application. So now we have, 29 executors.
// MAGIC
// MAGIC Note: On-heap memory stays inside the executor and requires garbage collection whereas Off-heap memory stays outside of executor and doesn't require garbage collection, and also it is mainly used for optimization purposes.

// COMMAND ----------

// MAGIC %md
// MAGIC Spark UI - Jobs
// MAGIC ====
// MAGIC Let's say a file of size 8.2 GB runs on your spark cluster
// MAGIC When you call one Action, one Job is created.
// MAGIC and 132 Tasks are created because (8.2Gb * 1024 / 128 MB = 65.6 ~ 66 blocks)
// MAGIC total blocks in HDFS will be 66
// MAGIC
// MAGIC we have 2 stages each with 66 tasks (66 partitions in our RDD)
// MAGIC therefore, there are total 132 tasks
// MAGIC
// MAGIC Constraint: we can have at the max 10 executors. 
// MAGIC
// MAGIC 10 executors have to execute 66 tasks in Stage 1.
// MAGIC This means some of the work has to be done in serial order.
// MAGIC
// MAGIC If in executor there is 1 CPU core then only one task can be performed at a time. If in executor there are 4 CPU core then this executor can parallely run 4 tasks at a time.
// MAGIC
// MAGIC Based on the current setting, we can get 10 executors, containing 1 CPU core.
// MAGIC
// MAGIC parallelism =
// MAGIC number of executors * number of cpu cores each executor hold =
// MAGIC 10 * 1 = 10 task can be executed at the same time

// COMMAND ----------

// MAGIC %md
// MAGIC Static Resource Allocation
// MAGIC ==========================
// MAGIC
// MAGIC Command:
// MAGIC spark2-shell --conf spark.dynamicAllocation.enabled=false --master yarn --num-executors 20 --executor-cores 2 --executor-memory 2G
// MAGIC
// MAGIC Note: If you have a long running job then go with the Dynamic resource allocation

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

val random = new scala.util.Random
val start = 1 
val end = 40

//I have a big log new text file which is 1.5GB in size
val rdd1 = sc.textFile("dbfs:/FileStore/tables/week12/bigLogNew.txt")

val rdd2 = rdd1.map(x => {
  var num = start + random.nextInt((end-start)+1)
  (x.split(":")(0)+num, x.split(":")(1))
})

val rdd3 = rdd2.groupByKey

val rdd4 = rdd3.map(x => (x._1, x._2.size))

val rdd5 = rdd4.map(x => {
  if(x._1.substring(0,4)=="WARN")
  ("WARN",x._2)
  else
  ("ERROR",x._2)
})

val rdd6 = rdd5.reduceByKey(_+_)

rdd6.collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Since I am using two wide transformations, that's why I have 3 stages (one for groupByKey and other for reduceByKey). 

// COMMAND ----------

print(rdd6.collect.foreach(println))

// COMMAND ----------

// MAGIC %md
// MAGIC Memory in Apache Spark
// MAGIC ======================
// MAGIC
// MAGIC Memory usage in spark falls under two broad categories - 
// MAGIC 1. Execution Memory 
// MAGIC > required for computations in shuffle, join, sorts, aggregations
// MAGIC
// MAGIC 2. Storage Memory
// MAGIC > mostly used for caching, broadcast..
// MAGIC
// MAGIC In Spark, both the memory share a common region. When no execution is happening, then your storage can acquire all the available memory and vice versa. Execution may evict storage if necessary. Let's say there is 2GB common unified region and there is no execution, so the storage is using 2GB and now if execution happens, then it will evict the memory taken by storage and this eviction will happen when the total storage memory usage falls under a certain threshold. That means there is a certain threshold beyond which the execution cannot evict the storage. 
// MAGIC There are other memories too but above two are the main.
// MAGIC
// MAGIC
// MAGIC This design ensures several desirable properties:
// MAGIC 1. Applications which do not use caching can use the entire space for execution.
// MAGIC 2. Applications that do not use caching can reserve a minimum storage space. means later if you want to use caching then this makes your data blocks immune from being evicted.

// COMMAND ----------

// MAGIC %md
// MAGIC A Scenario
// MAGIC ==========
// MAGIC
// MAGIC 1. if you request a container/executor of 4Gb size, then you are actually requesting 4GB (heap memory) + max(384MB or 10% of 4GB whichever is bigger) (off-heapmemory) -- overhead
// MAGIC 2. Out of the 4GB (total heap memory), 300MB is again reserved (3.7GB)
// MAGIC 3. Out of the 3.7GB, 60% of it goes to the unified storage (storage + execution memory) (2.3GB)
// MAGIC 4. Remaining 40% of 3.7GB goes to user memory (1.4GB) (to hold user datastructure, spark related metadata and safeguards OOM errors)

// COMMAND ----------

// MAGIC %md
// MAGIC Cache & Persist
// MAGIC ===============

// COMMAND ----------

rdd4.cache
//it is not cached yet until an action is called

// COMMAND ----------

rdd5.collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC Now, we can see in the storage and see that the rdd4 is cached
// MAGIC this is what i can see in Spark cluster UI - Master > Storage tab :
// MAGIC
// MAGIC RDDs
// MAGIC
// MAGIC ID	RDD Name	Storage Level	Cached Partitions	Fraction Cached	Size in Memory	Size on Disk
// MAGIC
// MAGIC 4	MapPartitionsRDD	Memory Deserialized 1x Replicated	22	100%	8.2 KiB	0.0 B
// MAGIC
// MAGIC Here, you can see that rdd4 is cached which has 22 partitions and all partitions are cached.| 
// MAGIC Note: In memory, it is serialized and in disk, it is desearialized. Persist works exactly the same way as cache.

// COMMAND ----------

import org.apache.spark.storage.StorageLevel

// COMMAND ----------

rdd4.persist(StorageLevel.DISK_ONLY)

// COMMAND ----------

rdd5.collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC since there are many storage levels, we are using disk only. and now after the action you will see under the storage, memory serialized.
// MAGIC
// MAGIC Here is the result copied from Spark cluster UI - master > storage tab:
// MAGIC
// MAGIC RDDs
// MAGIC
// MAGIC ID	RDD Name	Storage Level	Cached Partitions	Fraction Cached	Size in Memory	Size on Disk
// MAGIC
// MAGIC 4	MapPartitionsRDD	Disk Serialized 1x Replicated	22	100%	0.0 B	5.3 KiB

// COMMAND ----------

rdd4.unpersist()

// COMMAND ----------

// MAGIC %md
// MAGIC Optimization Tips
// MAGIC =================
// MAGIC When we talk about Memory, the data is kept in deserialized form and in disk, it is kept in serialized form (Serialized means in Bytes and it takes a bit of time because it has to be deserialized).
// MAGIC Deserialized is Fast but it takes more space.
// MAGIC
// MAGIC In terms of Serializer, we should prefer kryo serializer over Java serializer. 
// MAGIC Whenever the data is stored on disk or has to be transferred over the disk, it has to be in serialized form. 
// MAGIC
// MAGIC If we use Kryo serializer, then size will be much lesser than in the case of Java serializer.
// MAGIC
// MAGIC Kryo is always preferred for an advance level optimization, because it is faster and more compact than Java serialization (often as much as 10 times..)

// COMMAND ----------

// MAGIC %md
// MAGIC Questions
// MAGIC =========
// MAGIC
// MAGIC 1. Suppose we are running a simple wordcount problem in Spark on a cluster. The input file size is 100 GB. How many total tasks in all you can think can possibly run for this spark application, given there are two stages?
// MAGIC > Number of partitions = Number of blocks = (100GB * 1024 MB)/128 MB = 800. So each stage will have 800 partitions. Total 2 stages, so we have 800 * 2 = 1600 partitions. 
// MAGIC Total 1600 tasks will run for this spark application.
// MAGIC
// MAGIC 2. Suppose we are running a simple wordcount problem in Spark on a cluster. The input file size is 100GB. Suppose we have at the max 10 Executors running and one executor can have 4 CPU cores. For each stage what is the parallelism level achieved?
// MAGIC > Parallelism = Number of executors * Number of CPU cores that each executor holds. Total we have 800 tasks per stage. For 800 tasks, we have 10 executors running. Each executor has 4 CPU cores, so 4 tasks can run in parallel.
// MAGIC So, overall we can have 4 * 10 = 40 tasks running in parallel at a time in the cluster for each stage.

// COMMAND ----------


