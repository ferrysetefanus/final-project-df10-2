# -*- coding: utf-8 -*-
"""df10.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1L0632UUvE2c9iQTZqUAVG8t80yUGW21U
"""



import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType


import pandas as pd
import numpy as np



spark = SparkSession.builder \
    .appName('spark_transform') \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

df = spark.read.csv('/opt/airflow/datasets/online_payment.csv', header=True)

df.printSchema()

# Cast "step" column to IntegerType
df = df.withColumn("step", df["step"].cast(IntegerType()))

# start time (misalnya: '2023-07-01 00:00:00')
start_time = "2023-07-01 00:00:00"

# Convert "start_time" to TimestampType using F.lit()
start_time = F.lit(start_time).cast(TimestampType())

# convert step to timestamp
df = df.withColumn("timestamp", start_time + (F.col("step")) * F.expr("INTERVAL 1 HOUR"))

# create column date from timestamp
df = df.withColumn("date", F.date_format("timestamp", "yyyy-MM-dd"))

# Menggabungkan semua partisi menjadi satu partisi tunggal
df = df.coalesce(1)

# Ubah ke Pandas DataFrame
df_pandas = df.toPandas()

# Menyimpan DataFrame gabungan ke dalam satu file CSV tunggal
output_file_path = '/opt/airflow/datasets/online_payment.csv'
df_pandas.to_csv(output_file_path, index=False, mode='w')