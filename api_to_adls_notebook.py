# Databricks Notebook (PySpark)
import requests
import pandas as pd
from pyspark.sql import SparkSession
from io import StringIO
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Step 1: API Call
response = requests.get("https://jsonplaceholder.typicode.com/posts")
data = response.json()
df_pd = pd.DataFrame(data)

# Step 2: Convert to Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Step 3: Save to ADLS Gen2 (Delta Format)
output_path = "abfss://container@account.dfs.core.windows.net/demo/api_data"
df_spark.write.format("delta").mode("overwrite").save(output_path)
