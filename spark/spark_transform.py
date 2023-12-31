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
from pyspark.sql.functions import col, when, substring


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

# Fungsi untuk menentukan tipe (customer atau merchant)
def determine_type(char):
    return when(char == "C", "Customer").when(char == "M", "Merchant")

# Ekstrak karakter pertama dan buat kolom baru
df = df.withColumn("origin", determine_type(substring(col("nameOrig"), 1, 1))) \
              .withColumn("destination", determine_type(substring(col("nameDest"), 1, 1)))

# Fungsi untuk mapping nilai 0 dan 1 menjadi "No" dan "Yes"
def map_to_yes_no(value):
    return when(value == 0, "No").when(value == 1, "Yes")

# Lakukan mapping dan buat kolom baru
df = df.withColumn("isFraud", map_to_yes_no(col("isFraud"))) \
              .withColumn("isFlaggedFraud", map_to_yes_no(col("isFlaggedFraud")))

# Menggabungkan semua partisi menjadi satu partisi tunggal
df = df.coalesce(1)

# Ubah ke Pandas DataFrame
df_pandas = df.toPandas()

# Menyimpan DataFrame gabungan ke dalam satu file CSV tunggal
output_file_path = '/opt/airflow/datasets/online_payment.csv'
df_pandas.to_csv(output_file_path, index=False, mode='w')