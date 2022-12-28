# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore Udemy Data
# MAGIC ##### Objective: Exploratory Data Analysis of Udemy Data using PySpark + SparkSQL
# MAGIC 
# MAGIC ##### Data Source
# MAGIC   [Kaggle Udemy Data](https://www.kaggle.com/datasets/andrewmvd/udemy-courses)
# MAGIC   
# MAGIC ##### Key Fields / Columns
# MAGIC   - course_title
# MAGIC   - price
# MAGIC   - num_subscribers
# MAGIC   - level
# MAGIC   - subject
# MAGIC 
# MAGIC **Data Exploration goals**
# MAGIC   - Check Count
# MAGIC     - Total Records
# MAGIC     - Distinct Titles 
# MAGIC     - Distinct Level
# MAGIC     - Distinct Subjects
# MAGIC   - Derive Aggregation
# MAGIC     - Revenue by Subjects
# MAGIC   - Distribution
# MAGIC     - Top Subscribers by Subjects | Levels
# MAGIC     - Top Revenue generating Subjects
# MAGIC     - Find Top Courses by Number of Lectures | Duration
# MAGIC     - Find most subscribed Free Courses
# MAGIC   - Advance questions
# MAGIC     - Search given Text/Keywords in Courses
# MAGIC     - Word count / Word Map for keywaords in Course Titles
# MAGIC     - Which course Title(s) are repeating in data
# MAGIC     - Course publish Date (Year / month) distribution      
# MAGIC   

# COMMAND ----------

# All imports at one place
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import col, lit, trim, to_timestamp, from_unixtime, unix_timestamp, explode, split, length,lower, year,month

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Data

# COMMAND ----------

# Define Schema for dataset
schema = StructType([
                StructField("course_id",IntegerType(),False),
                StructField("course_title",StringType(),False),
                StructField("url",StringType(),False),
                StructField("is_paid",BooleanType(),False),
                StructField("price",FloatType(),False),
                StructField("num_subscribers",IntegerType(),False),
                StructField("num_reviews",IntegerType(),False),
                StructField("num_lectures",IntegerType(),False),
                StructField("level",StringType(),False),
                StructField("content_duration",FloatType(),False),
                StructField("published_timestamp",StringType(),False),
                StructField("subject",StringType(),False)
            ])

# COMMAND ----------

#Read the dataset (from DBFS file system where it was uploaded manually)
df = spark \
        .read \
        .option("header",True) \
        .schema(schema) \
        .csv("/FileStore/tables/udemy/udemy_courses.csv")
    
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Data

# COMMAND ----------

# 1. Remove Trailing spaces
# 2. Drop columns which are not required for analysis
# 3. Format TimeStamp

clean_df = df \
            .withColumn("course_title",trim(col("course_title")) ) \
            .withColumn("subject",trim(col("subject")) ) \
            .withColumn("level",trim(col("level")) ) \
            .withColumn("published_timestamp", to_timestamp(col("published_timestamp"),"yyyy-MM-dd'T'HH:mm:ss'Z'"))  \
            .withColumn("published_Year", year(col("published_timestamp")) )  \
            .withColumn("published_Month", month(col("published_timestamp")) )  \
            .drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explore Data

# COMMAND ----------

# For exploration, I have chosen to use Spark SQL. 
# Hence creating a Temporary View, which will be used in most of the code block below

clean_df.createOrReplaceTempView("V_UdemyCourses")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Total Records

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   V_UdemyCourses

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distinct Titles
# MAGIC Observation:
# MAGIC   - Out of 3683 records, 3668 are unique Titles
# MAGIC   - Let's find which titles are repeating

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(DISTINCT course_title)
# MAGIC FROM
# MAGIC   V_UdemyCourses

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distinct Learn Levels
# MAGIC Observation:
# MAGIC   - 4 learning Levels

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    DISTINCT level
# MAGIC FROM
# MAGIC   V_UdemyCourses

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Distinct Subjects
# MAGIC Observation:
# MAGIC   - Looks like few records have no Subbject mentioned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT subject
# MAGIC FROM
# MAGIC   V_UdemyCourses

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Looks like there are some bad records with no 'subject' mentioned
# MAGIC SELECT 
# MAGIC   count(*) 
# MAGIC FROM 
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   subject IS null

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Courses with No Subscribers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   num_subscribers = 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Revenue By Subject
# MAGIC Observation:
# MAGIC   - Max Revenue coming from Web Development Courses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   subject,
# MAGIC   sum(num_subscribers * price) AS RevenueBySubject
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   subject IS NOT NULL
# MAGIC GROUP BY
# MAGIC   subject
# MAGIC ORDER BY
# MAGIC   RevenueBySubject DESC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Distribution

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Subscribers By Subjects
# MAGIC Observation:
# MAGIC   - Web Development and Business Finance are winners

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   subject,
# MAGIC   count(num_subscribers) AS TotalSubscriberBySubject
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC GROUP BY
# MAGIC   subject
# MAGIC ORDER BY
# MAGIC   TotalSubscriberBySubject DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Subscribers by Level
# MAGIC Observation:
# MAGIC   - Most of the courses are for All / Begineer Level
# MAGIC   - only 58 Courses are for Expert Level learners

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   level,
# MAGIC   count(num_subscribers) as TotalSubscribers
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC GROUP BY
# MAGIC   level
# MAGIC ORDER BY
# MAGIC   TotalSubscribers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Revenue generating Titles
# MAGIC By Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   course_title,
# MAGIC   subject,
# MAGIC   sum(num_subscribers * price) AS TotalRevenue
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC GROUP BY
# MAGIC   course_title,
# MAGIC   subject
# MAGIC ORDER BY
# MAGIC   TotalRevenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Expert Level Courses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   subject,
# MAGIC   count(num_subscribers) as TotalSubscribers
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   level = 'Expert Level'
# MAGIC GROUP BY
# MAGIC   subject
# MAGIC ORDER BY
# MAGIC   TotalSubscribers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 Reviewed Courses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC ORDER BY
# MAGIC   num_reviews DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 Free Courses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   is_paid is false
# MAGIC ORDER BY
# MAGIC   num_subscribers desc
# MAGIC Limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 Lengthy Courses
# MAGIC Courses with Max Lectures & Duration
# MAGIC Observation:
# MAGIC   - There are few courses with > 200 Lectures !!!
# MAGIC   - Content Duration can be more  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Winner is "-- Winner is "The Complete Figure Drawing Course HD" with 78 hours of learing" with 779 lectures and 45 hours of learning
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC ORDER BY
# MAGIC   num_lectures DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Winner is "The Complete Figure Drawing Course HD" with 225 lectures and 78 hours of learning
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC ORDER BY
# MAGIC   content_duration DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Least Subscribed By Subject

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   subject,
# MAGIC   count(*) NumberOfCourses
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   num_subscribers = 0
# MAGIC GROUP BY
# MAGIC   subject

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Shortest Courses

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC ORDER BY
# MAGIC   content_duration asc

# COMMAND ----------

# MAGIC %md
# MAGIC #### Advanced Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Search Keywords in Titles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC WHERE
# MAGIC   course_title like '%Arabic%'
# MAGIC   OR
# MAGIC   course_title like '%Java%'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Keywords in Course Titles

# COMMAND ----------

stopwords_df = spark \
                .read \
                .option("header",False) \
                .csv("/FileStore/tables/standard/stopwords.csv") \
                .withColumn("stopword", lower(col("_c0")) )
                

word_df = clean_df \
            .withColumn('words',explode(split(col('course_title'), ' ')) )\
            .withColumn('lowerCaseWords', lower(col("words")) ) \
            .groupBy('lowerCaseWords')\
            .count()

join_word_df = word_df \
                    .join(stopwords_df,word_df["lowerCaseWords"] == stopwords_df["stopword"],"left")

final_wordcount_df  = join_word_df\
                        .filter(col("stopword").isNull()) \
                        .filter(length(col("lowerCaseWords")) != 1 ) \
                        .filter(length(col("lowerCaseWords")) != 0) \
                        .filter(~col("lowerCaseWords").isin(["learn","complete","beginners","beginner","introduction","build"]) ) \
                        .drop("stopword","_c0") \
                        .orderBy(col("count").desc()) \
                        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Year wise Published Cources

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   published_Year,
# MAGIC   count(course_title) AS CourseReleaseByYear
# MAGIC FROM
# MAGIC   V_UdemyCourses
# MAGIC GROUP BY
# MAGIC   published_Year
# MAGIC ORDER BY
# MAGIC   CourseReleaseByYear DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Duplicate Course Titles in data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Limitation to this approach is I can't selct all columns of duplicate records.
# MAGIC -- Alternate appprach is to save the dataframe as a table, and then use Self Join / Subquery.
# MAGIC With v_RepeatCources as (
# MAGIC     SELECT 
# MAGIC       course_title,
# MAGIC       count(course_title) as FreqOfCourseTitle
# MAGIC     FROM 
# MAGIC       V_UdemyCourses
# MAGIC     GROUP BY
# MAGIC       course_title
# MAGIC     ORDER BY
# MAGIC       FreqOfCourseTitle DESC)
# MAGIC select * from v_RepeatCources where FreqOfCourseTitle > 1
