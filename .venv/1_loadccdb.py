from lib2to3.pytree import convert
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
import mysql.connector

with open("./secret.txt") as f:
    secret_ls = f.readlines()
    user_name = secret_ls[0][:-1]
    user_password = secret_ls[1]

spark = SparkSession.builder.appName('Capstone').getOrCreate()
print('SparkSession created')

mydb = mysql.connector.connect(
  host="localhost",
  user=user_name,
  password=user_password,
  database="creditcard_capstone"
)
print('Connected to DB')

mycursor = mydb.cursor()

df_branch = spark.read.json("./cdw_sapp_branch.json")
df_branch = df_branch.toPandas()
df_branch['BRANCH_PHONE'] = df_branch['BRANCH_PHONE'].str.replace(r'^(\d{3})(\d{3})(\d+)$', r'(\1)\2-\3', regex=True)
df_branch['BRANCH_ZIP'] = df_branch['BRANCH_ZIP'].fillna(00000)
df_branch = spark.createDataFrame(df_branch)

df_credit = spark.read.json("./cdw_sapp_credit.json")
df_credit = df_credit.toPandas()
df_credit['MONTH'] = df_credit['MONTH'].astype(str).str.zfill(2)
df_credit['DAY'] = df_credit['DAY'].astype(str).str.zfill(2)
df_credit['TIMEID'] = df_credit['YEAR'].astype(str) + \
                      df_credit['MONTH'].astype(str) + \
                      df_credit['DAY'].astype(str)
df_credit.rename(columns={'CREDIT_CARD_NO':'CUST_CC_NO'}, inplace=True)

df_credit = spark.createDataFrame(df_credit)

df_customer = spark.read.json("./cdw_sapp_custmer.json")
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

mycursor.execute("""DROP TABLE IF EXISTS CDW_SAPP_BRANCH""")
mycursor.execute("""DROP TABLE IF EXISTS CDW_SAPP_CREDIT_CARD""")
mycursor.execute("""DROP TABLE IF EXISTS CDW_SAPP_CUSTOMER""")
mydb.commit()

df_branch.write.format("jdbc") \
               .mode("append") \
               .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
               .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
               .option("user", user_name) \
               .option("password", user_password) \
               .save()
print('Data written to table CDW_SAPP_BRANCH')

df_credit.write.format("jdbc") \
               .mode("append") \
               .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
               .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
               .option("user", user_name) \
               .option("password", user_password) \
               .save()
print('Data written to table CDW_SAPP_CREDIT_CARD')

df_customer.write.format("jdbc") \
               .mode("append") \
               .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
               .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
               .option("user", user_name) \
               .option("password", user_password) \
               .save()
print('Data written to table CDW_SAPP_CUSTOMER')

spark.stop()
print('SparkSession ended')

mydb.close()