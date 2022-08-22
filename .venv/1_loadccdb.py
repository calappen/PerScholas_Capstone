from lib2to3.pytree import convert
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
# import pyspark.sql.functions as F
# import pandas as pd
import numpy
spark = SparkSession.builder.appName('Capstone').getOrCreate()
# pd.set_option('max_columns', None)

df_branch = spark.read.json("./.venv/cdw_sapp_branch.json")
df_branch = df_branch.toPandas()
df_branch['BRANCH_PHONE'] = df_branch['BRANCH_PHONE'].str.replace(r'^(\d{3})(\d{3})(\d+)$', r'(\1)\2-\3', regex=True)
# print('nulls:', df_branch['BRANCH_ZIP'].isna().sum())
df_branch['BRANCH_ZIP'] = df_branch['BRANCH_ZIP'].fillna(00000)
df_branch = spark.createDataFrame(df_branch)
# print(df_branch.show())

df_credit = spark.read.json("./.venv/cdw_sapp_credit.json")
df_credit = df_credit.toPandas()
df_credit['MONTH'] = df_credit['MONTH'].astype(str).str.zfill(2)
df_credit['DAY'] = df_credit['DAY'].astype(str).str.zfill(2)
df_credit['TIMEID'] = df_credit['YEAR'].astype(str) + \
                      df_credit['MONTH'].astype(str) + \
                      df_credit['DAY'].astype(str)
df_credit.rename(columns={'CREDIT_CARD_NO':'CUST_CC_NO'}, inplace=True)
# print(df_credit.head())
df_credit = spark.createDataFrame(df_credit)
# print(df_credit.show())

df_customer = spark.read.json("./.venv/cdw_sapp_custmer.json")
df_customer = df_customer.toPandas()
df_customer['FIRST_NAME'] = df_customer['FIRST_NAME'].str.title()
df_customer['MIDDLE_NAME'] = df_customer['MIDDLE_NAME'].str.lower()
df_customer['LAST_NAME'] = df_customer['LAST_NAME'].str.title()
df_customer['FULL_STREET_ADDRESS'] = df_customer['APT_NO'].astype(str) + \
                                     ',' + \
                                     df_customer['STREET_NAME']
df_customer['CUST_PHONE'] = df_customer['CUST_PHONE'].astype(str).str.zfill(10)
df_customer['CUST_PHONE'] = df_customer['CUST_PHONE'].str.replace(r'^(\d{3})(\d{3})(\d+)$', r'(\1)\2-\3', regex=True)
df_customer = spark.createDataFrame(df_customer)
# print(df_customer.show())

df_branch.write.format("jdbc") \
 .mode("append") \
 .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
 .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
 .option("user", "root") \
 .option("password", "Pass1234") \
 .save()

df_credit.write.format("jdbc") \
 .mode("append") \
 .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
 .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
 .option("user", "root") \
 .option("password", "Pass1234") \
 .save()

df_customer.write.format("jdbc") \
 .mode("append") \
 .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
 .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
 .option("user", "root") \
 .option("password", "Pass1234") \
 .save()

spark.stop()