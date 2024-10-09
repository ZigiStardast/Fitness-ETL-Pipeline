import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lead, lag, col

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("spark_session") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

parsed_data = "/opt/airflow/spark_files/parsed_data_daily_activity.csv"

if os.path.exists(parsed_data):
    spark_df = spark.read.csv(parsed_data,
                        header='true',
                        inferSchema='true',
                        ignoreLeadingWhiteSpace=True,
                        ignoreTrailingWhiteSpace=True)
else:
    raise FileNotFoundError(f"Datoteka ne postoji na putanji: {parsed_data}") 


spark_df = spark_df.withColumn("ActivityDate", F.to_date("ActivityDate", "MM/dd/yyyy"))
spark_df = spark_df.withColumnRenamed("ActivityDate", "dateFor")

spark_df_activity = spark_df.selectExpr(
    "Id",
    "dateFor",
    "TotalSteps",
    "TotalDistance",
    "Calories"
)

windowSpec = Window.partitionBy("Id").orderBy(F.col('dateFor').asc())

df_with_diff = spark_df_activity \
    .withColumn("TotalStepsDiff", F.coalesce(F.col("TotalSteps") - F.lag("TotalSteps", 1).over(windowSpec), F.lit(0)) ) \
    .withColumn("TotalDistanceDiff", F.coalesce(F.col("TotalDistance") - F.lag("TotalDistance", 1).over(windowSpec), F.lit(0)) ) \
    .withColumn("CaloriesDiff", F.coalesce(F.col("Calories") - F.lag("Calories", 1).over(windowSpec), F.lit(0)) )

'''
total_minutes = spark_df \
        .withColumn("TotalMinutes", col("VeryActiveMinutes") + col("FairlyActiveMinutes") + col("LightlyActiveMinutes") + col("SedentaryMinutes"))


df_activity_percentage = total_minutes.selectExpr(
    "Id",
    "dateFor",
    "VeryActiveMinutes / TotalMinutes * 100 as VeryActivePercentage",
    "FairlyActiveMinutes / TotalMinutes * 100 as FairlyActivePercentage",
    "LightlyActiveMinutes / TotalMinutes * 100 as LightlyActivePercentage",
    "SedentaryMinutes / TotalMinutes * 100 as SedentaryPercentage"
)
'''

results_diff = df_with_diff.fillna(0)
# results_activity = df_activity_percentage.fillna(0)


results_diff.toPandas().to_csv('/opt/airflow/spark_files/results.csv',
                         sep=',',
                         header=True,
                         index=False)
'''
results_activity.toPandas().to_csv('/opt/airflow/spark_files/results_activity.csv',
                         sep=',',
                         header=True,
                         index=False)
'''
os.remove(parsed_data)




