// Databricks notebook source
// Q2 [25 pts]: Analyzing a Large Graph with Spark/Scala on Databricks

// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Definfing the data schema
val customSchema = StructType(Array(StructField("answerer", IntegerType, true), StructField("questioner", IntegerType, true),
    StructField("timestamp", LongType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
// MAKE SURE THAT YOU REPLACE THE examplegraph.csv WITH THE mathoverflow.csv FILE BEFORE SUBMISSION.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "false") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/mathoverflow.csv")
  .withColumn("date", from_unixtime($"timestamp"))
  .drop($"timestamp")

// COMMAND ----------

//display(df)
df.show()

// COMMAND ----------

// PART 1: Remove the pairs where the questioner and the answerer are the same person.
// ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

// ENTER THE CODE BELOW
//df.filter($"answerer">1).show()
val DF1=df.filter("answerer!=questioner")
// val DF11=df.dropDuplicates("answererquestioner")
DF1.show()
// DF11.show()


// COMMAND ----------

// PART 2: The top-3 individuals who answered the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest out-degrees.

// ENTER THE CODE BELOW
val DF2=DF1.groupBy("answerer").count.withColumnRenamed("count","questions_answered").sort(desc("questions_answered")).limit(3)
DF2.show()


// COMMAND ----------

// PART 3: The top-3 individuals who asked the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest in-degree.

// ENTER THE CODE BELOW
val DF3=DF1.groupBy("questioner").count.withColumnRenamed("count","questions_asked").sort(desc("questions_asked"),asc("questioner")).limit(3)
DF3.show()

// COMMAND ----------

// PART 4: The top-5 most common asker-answerer pairs - sorted in descending order - if tie, the one with lower value node-id in the first column (u->v edge, u value) gets listed first.

// ENTER THE CODE BELOW
val DF4=DF1.groupBy("answerer","questioner").count.sort(desc("count"),asc("answerer"),asc("questioner")).limit(5)
DF4.show()

// COMMAND ----------

// PART 5: Number of interactions (questions asked/answered) over the months of September-2010 to December-2010 (i.e. from September 1, 2010 to December 31, 2010). List the entries by month from September to December.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint: Check how we extracted the date from the timestamp.

// ENTER THE CODE BELOW
// val DF5 = DF1.select(month("date").alias("month")).show()
// val DF5=DF1.select(month(col("date")))
//var DF6=DF1.filter(month(col("date"))==8)
var DF5=DF1.filter(month(col("date"))>8 and year(col("date"))>2009 and year(col("date"))<2011)

//var DF52=DF5.groupBy(month(col("date"))).count.withColumnRenamed("month(date)","month").withColumnRenamed("count","total_interactions")
var DF53=DF5.groupBy(month(col("date"))).count.withColumnRenamed("month(date)","month").withColumnRenamed("count","total_interactions").sort(asc("month"))
DF53.show()

// COMMAND ----------

// PART 6: List the top-3 individuals with the maximum overall activity, i.e. total questions asked and questions answered.

// ENTER THE CODE BELOW
val 
DF61=DF1.groupBy("answerer").count.withColumnRenamed("answerer","userID").sort(desc("count"),asc("answerer"))
//DF61.show()
var DF62=DF1.groupBy("questioner").count.withColumnRenamed("answerer","userID").sort(desc("count"),asc("questioner"))
//DF62.show()
val DF63=DF61.union(DF62)
// DF63.show()
var DF64=DF63.groupBy("userID").agg(sum("count").as("total_activity")).sort(desc("total_activity"),asc("userID")).limit(3)
// val DF63=DF61.join(DF62,"userID").sum("count")
DF64.show()
