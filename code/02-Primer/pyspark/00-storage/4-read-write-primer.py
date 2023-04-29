# Databricks notebook source
# MAGIC %md
# MAGIC # DBFS - read/write primer
# MAGIC In this exercise, we will:<br>
# MAGIC 1.  Read Chicago crimes public dataset from csv - 1.5 GB of the Chicago crimes public dataset - has 6.7 million records.<BR>
# MAGIC 2.  Read the CSV into a dataframe, **persist as parquet** to the raw directory<BR>
# MAGIC 3.  **Write the data frame into a unity catalog table**<BR>
# MAGIC 4.  **Explore with SQL construct**<BR>
# MAGIC 5.  **Curate** the dataset (dedupe, add additional dervived attributes of value etc) for subsequent labs<BR>
# MAGIC 6.  Do some basic **visualization**<BR>
# MAGIC
# MAGIC Chicago crimes dataset:<br>
# MAGIC Website: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2<br>
# MAGIC Dataset: https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD<br>
# MAGIC Metadata: https://cosmosdbworkshops.blob.core.windows.net/metadata/ChicagoCrimesMetadata.pdf<br>
# MAGIC   
# MAGIC Referenes for Databricks:<br>
# MAGIC Visualization: https://docs.databricks.com/user-guide/visualizations/charts-and-graphs-scala.html

# COMMAND ----------

# Import pyspark utility functions
from pyspark.sql import functions as F
# Name functions enables automatic env+user specific database naming
from libs.dbname import dbname
from libs.tblname import tblname, username
uname = username(dbutils)

# 3) List to validate if file exists
dbfs_src_dir_path = f"/mnt/workshop/staging/crimes/chicago-crimes"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Ensure source data in dbfs is available

# COMMAND ----------

display(dbutils.fs.ls(dbfs_src_dir_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read raw CSV, persist to parquet

# COMMAND ----------

# 2) Destination directory
dbfs_dest_dir_path_raw = f"/mnt/workshop/users/{uname}/raw/crimes/chicago-crimes"

# COMMAND ----------

# 3) Check first few lines
dbutils.fs.head(dbfs_src_dir_path + "/chicago-crimes.csv")

# COMMAND ----------

# 4)  Read raw CSV
sourceDF = (spark.read.format("csv")
    .options(header='true', delimiter = ',')
    .load(dbfs_src_dir_path).toDF(  # Pass field names for columns
        "case_id", "case_nbr", "case_dt_tm", "block", "iucr", "primary_type", "description", "location_description", "arrest_made", "was_domestic", "beat", "district", "ward", "community_area", "fbi_code", "x_coordinate", "y_coordinate", "case_year", "updated_dt", "latitude", "longitude", "location_coords")
)

sourceDF.printSchema()
display(sourceDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist the dataset to parquet
# MAGIC
# MAGIC Parquet is an open-source binary format to store columnar and unstructured data, like csv, tables or json,
# MAGIC in an efficient and type safe manner.
# MAGIC
# MAGIC This enables consumption outside of unity catalog.
# MAGIC Not necessary to persist our data in delta lake format, but can be useful for other use cases.

# COMMAND ----------

# 5) Persist as parquet to raw zone
dbutils.fs.rm(dbfs_dest_dir_path_raw, recurse=True)
sourceDF.coalesce(2).write.parquet(dbfs_dest_dir_path_raw)

# COMMAND ----------

display(dbutils.fs.ls(dbfs_dest_dir_path_raw))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to delta lake table in unity catalog

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Unity Catalog table names, set spark vars for use in sql queries
# MAGIC
# MAGIC `dbname()` and `tblname()` generate env and user specific dev-table names.
# MAGIC This enables us to develop our code without interrupting production env nor other users.
# MAGIC An example data set name is `training.dev_paldevibe_crime.chicago_crimes_raw`.
# MAGIC
# MAGIC Notice that this uses delta catalog three level structure:
# MAGIC
# MAGIC * catalog: `training`
# MAGIC * db/schema: `dev_paldevibe_crime`
# MAGIC * table: `chicago_crimes_raw`

# COMMAND ----------

# db name
crime = dbname(db="crime")
print("crime:" + repr(crime))
spark.conf.set("nbvars.crime", crime)

# chicago_crimes_raw table name
chicago_crimes_raw = tblname(db="crime", tbl="chicago_crimes_raw")
print("chicago_crimes_raw:" + repr(chicago_crimes_raw))
spark.conf.set("nbvars.chicago_crimes_raw", chicago_crimes_raw)

# chicago_crimes_curated table name
chicago_crimes_curated = tblname(db="crime", tbl="chicago_crimes_curated")
print("chicago_crimes_curated:" + repr(chicago_crimes_curated))
spark.conf.set("nbvars.chicago_crimes_curated", chicago_crimes_curated)
spark.conf.set("nbvars.dbfs_dest_dir_path_raw", dbfs_dest_dir_path_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Coalesce data and view schema
# MAGIC Coalesce reduces or increases amount of partitions to make write more efficient.
# MAGIC We split into 8 partitions to better distribute writing.

# COMMAND ----------

coalesced = sourceDF.coalesce(8)
# show schema with pyspark
coalesced.schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write raw data to new unity catalog table

# COMMAND ----------

coalesced.write.mode("overwrite").format("delta").saveAsTable(chicago_crimes_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Explore the raw dataset with sql
# MAGIC
# MAGIC Use injected var for dynamic table naming.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ${nbvars.chicago_crimes_raw};
# MAGIC
# MAGIC --6,701,049

# COMMAND ----------

# MAGIC  %md
# MAGIC ### 6. Curate the dataset
# MAGIC  In this section, we will just parse the date and time for the purpose of analytics.

# COMMAND ----------

# 1) Read and curate
# Lets add some temporal attributes that can help us analyze trends over time

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,FloatType,DoubleType, TimestampType, DecimalType
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, udf

# Temp view names are local to notebooks, we create one called raw_crimes here
spark.sql(f"select * from {chicago_crimes_raw}").withColumn(
    "case_timestamp",
    to_timestamp("case_dt_tm","MM/dd/yyyy hh:mm:ss a")).createOrReplaceTempView("raw_crimes")
curated_initial_df = (spark.sql("""
SELECT *, 
month(case_timestamp) as case_month,
dayofmonth(case_timestamp) as case_day_of_month, 
hour(case_timestamp) as case_hour, 
dayofweek(case_timestamp) as case_day_of_week_nbr from raw_crimes""")
)
curated_df = curated_initial_df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Inspect the data set with pyspark display function
# MAGIC Most functionality is available both in sql and pyspark (and scala spark)

# COMMAND ----------

display(curated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Persist curated data to parquet
# MAGIC
# MAGIC This enables consumption outside of unity catalog.
# MAGIC Not necessary to persist data in delta lake format, but can be useful for other use cases.

# COMMAND ----------

# 2) Persist as parquet to curated storage zone, 
dbfs_dest_dir_path_curated = f"/mnt/workshop/users/{uname}/curated/crimes/chicago-crimes"
dbutils.fs.rm(dbfs_dest_dir_path_curated, recurse=True)
curated_df.write.partitionBy("case_year","case_month").parquet(dbfs_dest_dir_path_curated)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Persist curate data to delta lake table in unity catalog

# COMMAND ----------

# 3) Write to new unity catalog table
curated_df.write.mode("overwrite").format("delta").saveAsTable(chicago_crimes_curated)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inspect schema with sql describe command

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted ${nbvars.chicago_crimes_curated};

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inspect data with sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${nbvars.chicago_crimes_curated};
# MAGIC --select count(*) as crime_count from ${nbvars.chicago_crimes_curated} --where primary_type='THEFT';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 7. Report on the dataset/visualize
# MAGIC In this section, we will explore data and visualize

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by case_year in sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT case_year, count(*) AS crime_count FROM ${nbvars.chicago_crimes_curated}
# MAGIC GROUP BY case_year ORDER BY case_year;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by case_year in pyspark

# COMMAND ----------

grouped_by_year_df = curated_df.groupBy("case_year").count().orderBy("case_year")
display(grouped_by_year_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by case_year, primary_type, filter on specific types, using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cast(cast(case_year as string) as date) as case_year, primary_type as case_type, count(*) AS crime_count
# MAGIC FROM ${nbvars.chicago_crimes_curated}
# MAGIC where primary_type in ('BATTERY','ASSAULT','CRIMINAL SEXUAL ASSAULT')
# MAGIC GROUP BY case_year,primary_type ORDER BY case_year;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group by case_year, primary_type, filter on specific types, using pyspark

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

filtered_df = (
    curated_df.select(
        F.to_date(F.col("case_year").cast("string")).alias("case_year"),
        F.col("primary_type").alias("case_type"),
        F.count().alias("crime_count")
    ).where(F.col("primary_type").isin("BATTERY", "ASSAULT", "CRIMINAL SEXUAL ASSAULT"))
    .groupBy("case_year", "primary_type").orderBy("case_year")
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Filter on parts of string in crime type

# COMMAND ----------

# MAGIC %sql
# MAGIC select case_year,primary_type as case_type, count(*) as crimes_count
# MAGIC from ${nbvars.chicago_crimes_curated}
# MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%')
# MAGIC GROUP BY case_year, case_type
# MAGIC ORDER BY case_year,case_type desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select primary_type as case_type, count(*) as crimes_count
# MAGIC from ${nbvars.chicago_crimes_curated}
# MAGIC where (primary_type LIKE '%ASSAULT%' OR primary_type LIKE '%CHILD%') OR (primary_type='KIDNAPPING')
# MAGIC GROUP BY case_type;

# COMMAND ----------


