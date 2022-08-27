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


categories = cc_df['TRANSACTION_TYPE'].value_counts()
transaction_colors = set_colors(len(categories))
categories.plot(kind='barh', figsize=(10, 5),
                xlim=(categories.min()-500,categories.max()+100),
                color=transaction_colors, 
                title='Total Transactions Per Category')
for index, value in enumerate(categories):
    plt.text(value+10, index-0.1, str(value))
plt.show()


states = customer_df['CUST_STATE'].value_counts()
state_colors = set_colors(len(states))
states.plot(kind='barh', figsize=(10, 5), color=state_colors,
            title='Total Number of Customers Per State')
for index, value in enumerate(states):
    plt.text(value+0.6, index-0.4, str(value))
plt.show()

highest_20 = customer_cc_df.groupby('CUST_SSN')['TRANSACTION_VALUE'] \
                           .sum() \
                           .sort_values(ascending=False) \
                           .head(20)
highest_colors = set_colors(len(highest_20))
highest_20.plot(kind='barh', figsize=(10, 5),
                xlim=(highest_20.min()-200,highest_20.max()+100),
                color=highest_colors, xlabel='Account Number',
                title='Top 20 Total Transaction Amounts Per Customer')
for index, value in enumerate(highest_20):
    plt.text(value+10, index-0.25, '$'+str(value))
plt.show()


top_3_months = cc_df['MONTH'].value_counts() \
                             .sort_values(ascending=False) \
                             .head(3)
top_colors = set_colors(len(top_3_months))
top_3_months.plot(kind='barh', figsize=(10, 5),
                  xlim=(top_3_months.min()-50,top_3_months.max()+20),
                  color=top_colors, xlabel='Month',
                  title='Top 3 Total # of Transactions Per Month')
for index, value in enumerate(top_3_months):
    plt.text(value+1, index-0.04, str(value))
plt.show()


branch_health = cc_df[cc_df['TRANSACTION_TYPE']=='Healthcare'].groupby('BRANCH_CODE', as_index=False)['TRANSACTION_VALUE'] \
                                                              .sum() \
                                                              .sort_values(by='TRANSACTION_VALUE', ascending=False)
branch_colors = set_colors(len(branch_health))
branch_health.plot(kind='scatter', x='BRANCH_CODE', y='TRANSACTION_VALUE',
                   figsize=(30, 10), color=branch_colors, xlabel='Branch #',
                   ylabel='Total Value of Healthcare Transactions',
                   title='Total $ Value of Healthcare Transactions Per Branch')
branch_health_series = branch_health.set_index('BRANCH_CODE').squeeze()
for index, value in branch_health_series.items():
    plt.text(index+1, value-20, '#'+str(index))

# alternate method for labeling the scatter plot
# for i in range(len(branch_health)):
#     plt.text(x=branch_health['BRANCH_CODE'][i]+1,
#              y=branch_health['TRANSACTION_VALUE'][i]+20,
#              s=branch_health['BRANCH_CODE'][i])

plt.show()

spark.stop()