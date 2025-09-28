# Databricks notebook source
#Creating mount path storage 
storage_account_name = "storageaccountharshit"
container_name = "youtube"
mount_point = "/mnt/youtube/"
access_key = " ---- <Please mention Your Access Key> ----"

dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = mount_point,
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": access_key}
)
print(True)

# COMMAND ----------

#reading youtube 5 GB file
yt_raw_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/youtube/sourcedata/trending_yt_videos_113_countries.csv")

#Pulling only required columns
required_col = ['channel_name','view_count','like_count','comment_count','publish_date']

yt_raw_df = yt_raw_df.select(required_col)

yt_raw_df.show()

# COMMAND ----------

from pyspark.sql.functions import *
#Data Quality
yt_quality_df = (
yt_raw_df.withColumn("view_count",
when(col("view_count").rlike("^[0-9]+$"), col("view_count").cast("int")).otherwise(0))
.withColumn("like_count",
when(col("like_count").rlike("^[0-9]+$"), col("view_count").cast("int")).otherwise(0))
.withColumn("comment_count",
when(col("comment_count").rlike("^[0-9]+$"), col("view_count").cast("int")).otherwise(0))
.withColumn("publish_date", to_date(col("publish_date"), "yyyy-MM-dd"))
.withColumn("publish_date",
when(col("publish_date").isNotNull(), col("publish_date")).otherwise("1900-01-01"))
.withColumn("channel_name",when(col("channel_name").rlike("^[a-zA-Z]+$"),col("channel_name").cast("string")).otherwise(lit('others')))
)
yt_quality_df.show()

# COMMAND ----------

#group by columns
group_by_col = ['channel_name','publish_date']

#'view_count','like_count','comment_count'

#transformation
channel_agg_df = yt_quality_df.groupBy(*group_by_col).agg(sum("view_count").alias("sum_view_count"),sum("like_count").alias("sum_like_count"),sum("comment_count").alias("sum_comment_count"))
channel_agg_df.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, col

window_spec = Window.orderBy(desc("sum_view_count"))
channel_agg_df = channel_agg_df.filter("channel_name != 'others'").withColumn("rank_channel", row_number().over(window_spec))
top_10_channel_df = channel_agg_df.filter(col("rank_channel") < 10)
top_10_channel_df.show()