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

def set_colors(num):
    return ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(num)]

transaction_colors = set_colors(len(cc_df['TRANSACTION_TYPE'].unique()))
cc_df['TRANSACTION_TYPE'].value_counts().plot(kind='barh', figsize=(10, 5), color=transaction_colors, title='Total Transactions Per Category')
plt.show()

state_colors = set_colors(len(customer_df['CUST_STATE'].unique()))
customer_df['CUST_STATE'].value_counts().plot(kind='barh', figsize=(10, 5), color=state_colors, title='Total Number of Customers Per State')
plt.show()

highest_20 = customer_cc_df.groupby('CUST_SSN')['TRANSACTION_VALUE'] \
                           .sum() \
                           .sort_values(ascending=False) \
                           .head(20)
highest_colors = set_colors(len(highest_20))
highest_20.plot(kind='barh', figsize=(10, 5), color=highest_colors,
                title='Top 20 Total Transaction Amounts Per Customer')
plt.show()


top_3_months = cc_df['MONTH'].value_counts() \
                             .sort_values(ascending=False) \
                             .head(3)
top_colors = set_colors(len(top_3_months))
top_3_months.plot(kind='barh', figsize=(10, 5),
                  color=top_colors, xlabel='Month', ylabel='# of Transactions',
                  title='Top 3 Total # of Transaction Per Month')
plt.show()

branch_health = cc_df[cc_df['TRANSACTION_TYPE']=='Healthcare'].groupby('BRANCH_CODE', as_index=False)['TRANSACTION_VALUE'] \
                                                              .sum() \
                                                              .sort_values(by='TRANSACTION_VALUE', ascending=False)
branch_colors = set_colors(len(branch_health))
branch_health.plot(kind='scatter', x='BRANCH_CODE', y='TRANSACTION_VALUE',
                   figsize=(30, 10), color=branch_colors, xlabel='Branch #',
                   ylabel='Total Value of Healthcare Transactions',
                   title='Total $ Value of Healthcare Transactions Per Branch')

for i in range(len(branch_health)):
    plt.text(x=branch_health['BRANCH_CODE'][i]+1,
             y=branch_health['TRANSACTION_VALUE'][i]+20,
             s=branch_health['BRANCH_CODE'][i])

plt.show()

spark.stop()