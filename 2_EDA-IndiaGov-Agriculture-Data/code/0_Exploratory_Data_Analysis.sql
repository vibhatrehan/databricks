-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Load Data
-- MAGIC   - Create Dataframe
-- MAGIC   - Create Temporary View for SQL Analysis

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
-- MAGIC from pyspark.sql.functions import aggregate, sum, trim,col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -----------------------------------
-- MAGIC # Load Data
-- MAGIC # -----------------------------------
-- MAGIC 
-- MAGIC produce_schema = StructType([
-- MAGIC                     StructField("State_Name",StringType(),False),
-- MAGIC                     StructField("District_Name",StringType(),False),
-- MAGIC                     StructField("Crop_Year",IntegerType(),False),
-- MAGIC                     StructField("Season",StringType(),False),
-- MAGIC                     StructField("Crop",StringType(),False),
-- MAGIC                     StructField("Area",DoubleType(),False),   
-- MAGIC                     StructField("Production",DoubleType(),False)    
-- MAGIC                  ])
-- MAGIC 
-- MAGIC produce_df = spark\
-- MAGIC             .read\
-- MAGIC             .schema(produce_schema)\
-- MAGIC             .option("header",True)\
-- MAGIC             .csv("/FileStore/tables/india/agri-produce/Agri_produce_India.csv")
-- MAGIC 
-- MAGIC # -----------------------------------
-- MAGIC # Create Temp View
-- MAGIC # -----------------------------------
-- MAGIC produce_df\
-- MAGIC     .withColumn("State_Name",trim(col("State_Name")))\
-- MAGIC     .withColumn("District_Name",trim(col("District_Name")))\
-- MAGIC     .withColumn("Season",trim(col("Season")))\
-- MAGIC     .withColumn("Crop",trim(col("Crop")))\
-- MAGIC     .createOrReplaceTempView("v_temp_produce")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -----------------------------------
-- MAGIC # Print Schema
-- MAGIC # -----------------------------------
-- MAGIC produce_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -----------------------------------
-- MAGIC # Display Data
-- MAGIC # -----------------------------------
-- MAGIC produce_df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Profile
-- MAGIC   - `Uttar Pradesh` as **State** appear most frequently in the data
-- MAGIC   - `Buapur` as **district** appear most frequently in the data
-- MAGIC   - `Rice` is the most occuring **Crop** in data
-- MAGIC   - `Kharif` **Season** appears the most in data
-- MAGIC   
-- MAGIC   
-- MAGIC   | Data Point         | Details         |  Comments |
-- MAGIC   |--------------------|-----------------|----------|
-- MAGIC   | Total Data | 246K Records |
-- MAGIC   | Missing Data | 20K Records | Population Column|
-- MAGIC   | Crop Year | 1997 to 2015|
-- MAGIC   | Unique States | 31|
-- MAGIC   | Unique Districts | 618|
-- MAGIC   | Unique Crops| 130 |

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC -- ---------------------------------
-- MAGIC -- Create Data Profile
-- MAGIC -- ---------------------------------
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   v_temp_produce
-- MAGIC   
-- MAGIC -- 
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exploratory Data Anaysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Missing Data distribution

-- COMMAND ----------

-- Around 1300 combinations has Production data missing
Select 
  distinct(Crop_Year, Crop, State_Name) as distinct_combination
FROM
  v_temp_produce
WHERE
  Production IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Top Contenders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Most Produced Crops

-- COMMAND ----------

SELECT
  CROP,
  Sum(Production) AS totalProduce
FROM
  v_temp_produce
GROUP BY
  Crop
ORDER BY
  totalProduce DESC
LIMIT 5


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### State + Crop + Year

-- COMMAND ----------

-- ---------------------------------------------------------
-- All time high Produce by State, Crop, Year
-- ---------------------------------------------------------
SELECT
  State_Name,
  Crop,
  Crop_Year,
  round(SUM(Production),2) AS TotalProduceByCombination
FROM
  v_temp_produce
WHERE
  Production IS NOT NULL
GROUP BY
  State_Name,
  Crop,
  Crop_Year
ORDER BY
  TotalProduceByCombination DESC
LIMIT
  7

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Districts + Crop

-- COMMAND ----------

-- ---------------------------------------------------------
-- Coconut production appearing front runner. 
-- Can this be because of weight of Coconut will always be more than grains. 
-- Should grains be compared seperatly?
-- ---------------------------------------------------------
SELECT
  DISTRICT_NAME,
  CROP,
  sum(Production) AS totalDistrictLevelCropProduce
FROM
  V_TEMP_PRODUCE
GROUP BY
  DISTRICT_NAME,
  CROP
ORDER BY
  totalDistrictLevelCropProduce DESC
LIMIT 5


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Top Wheat and Rice Producers : All Time

-- COMMAND ----------

-- ---------------------------------------------------------
-- Wheat and Rice Production Frontrunners
-- ---------------------------------------------------------

SELECT
  STATE_NAME,
  DISTRICT_NAME,
  CROP,
  SUM(Production) as total_produce
FROM
  v_temp_produce
WHERE
  Crop in ("Wheat","Rice")
  and Production is not null
GROUP BY
  STATE_NAME,
  DISTRICT_NAME,
  CROP
ORDER BY
  total_produce DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Top Crops for each Season

-- COMMAND ----------

(
    SELECT
      Season,
      Crop,
      round(SUM(Production),2) AS TotalProduceByCombination
    FROM
      v_temp_produce
    WHERE
      Production IS NOT NULL
      AND Season = 'Whole Year'
    GROUP BY
      Season,Crop
    ORDER BY
      TotalProduceByCombination DESC
    LIMIT
     3
)
UNION
(
    SELECT
      Season,
      Crop,
      round(SUM(Production),2) AS TotalProduceByCombination
    FROM
      v_temp_produce
    WHERE
      Production IS NOT NULL
      AND Season = 'Winter'
    GROUP BY
      Season,Crop
    ORDER BY
      TotalProduceByCombination DESC
    LIMIT
     3
)
UNION
(
    SELECT
      Season,
      Crop,
      round(SUM(Production),2) AS TotalProduceByCombination
    FROM
      v_temp_produce
    WHERE
      Production IS NOT NULL
      AND Season = 'Kharif'
    GROUP BY
      Season,Crop
    ORDER BY
      TotalProduceByCombination DESC
    LIMIT
     3
)
UNION
(
    SELECT
      Season,
      Crop,
      round(SUM(Production),2) AS TotalProduceByCombination
    FROM
      v_temp_produce
    WHERE
      Production IS NOT NULL
      AND Season = 'Summer'
    GROUP BY
      Season,Crop
    ORDER BY
      TotalProduceByCombination DESC
    LIMIT
     3
)
ORDER BY
  Season,TotalProduceByCombination DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Most Fertile land : For Cocunut

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Top states for each crop type : Given Year's Data

-- COMMAND ----------

-- --------------------------------------------------------------------
-- E.g In 2010, Max production of Bajra happened in Rajasthan
-- --------------------------------------------------------------------

SELECT
  Crop,
  STATE_NAME,
  max(Production) AS totalProduce
FROM
  v_temp_produce
WHERE
  crop_year = 2010
  and Production is not null
GROUP BY
  STATE_NAME,
  Crop
ORDER BY
  Crop,
  totalProduce DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Distinct Seasons

-- COMMAND ----------

SELECT
  DISTINCT Season
FROM
  v_temp_produce

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Distinct Crops

-- COMMAND ----------

SELECT
  DISTINCT Crop
FROM
  v_temp_produce

-- COMMAND ----------

-- ------------------------------------------------
-- For each Crop, find the top producer State
-- -------------------------------------------------
SELECT
  Crop,
  STATE_NAME,
  max(Production) AS maxProduce
FROM
  v_temp_produce
WHERE
  Production is not null
GROUP BY
  STATE_NAME,
  Crop
ORDER BY
  Crop DESC,
  maxProduce DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visulaization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Yearly Produce trend for given crops

-- COMMAND ----------

-- -------------------------------------------------------------------
-- lOOKS LIKE DATA QUALITY ISSUE FOR 2015 DATA (Missing Data?? )
-- -------------------------------------------------------------------
SELECT
  Crop_Year,
  Crop,
  sum(Production) as total_produce
FROM
  v_temp_produce
WHERE
  Crop in ("Wheat","Rice","Groundnut")
GROUP BY
  Crop_Year,
  Crop

-- COMMAND ----------

Select * from v_temp_produce where crop_year=2015 and crop="Groundnut"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### State wise Produce for Crops (X,Y,Z)

-- COMMAND ----------

SELECT
  State_Name,
  Crop,
  sum(Production) as total_produce
FROM
  v_temp_produce
WHERE
  Crop in ("Wheat","Rice","Sugarcane")
GROUP BY
  State_Name,
  Crop
ORDER BY
  total_produce DESC
