from enum import unique
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import random

spark = SparkSession.builder.appName('Capstone').getOrCreate()

customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="Pass1234",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_customer").load().toPandas()

cc_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="Pass1234",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_credit_card").load().toPandas()

branch_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="Pass1234",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_branch").load().toPandas()

customer_cc_df = pd.merge(customer_df, cc_df, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')
branch_cc_df = pd.merge(branch_df, cc_df, how='inner', on='BRANCH_CODE')

number_of_colors = len(cc_df['TRANSACTION_TYPE'].unique())

my_colors = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(number_of_colors)]

cc_df['TRANSACTION_TYPE'].value_counts().plot(kind='barh', figsize=(10, 5), color=my_colors, title='Total Transactions Per Category')

plt.show() 

spark.stop()