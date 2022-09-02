import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import random

with open("./secret.txt") as f:
    secret_ls = f.readlines()
    user_name = secret_ls[0][:-1]
    user_password = secret_ls[1]

spark = SparkSession.builder.appName('Capstone').getOrCreate()
print('SparkSession created')

customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                              user=user_name,\
                                              password=user_password,\
                                              url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                              dbtable="creditcard_capstone.cdw_sapp_customer").load().toPandas()
print('Data read from table cdw_sapp_customer')

cc_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                        user=user_name,\
                                        password=user_password,\
                                        url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                        dbtable="creditcard_capstone.cdw_sapp_credit_card").load().toPandas()
print('Data read from table cdw_sapp_credit_card')

branch_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                            user=user_name,\
                                            password=user_password,\
                                            url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                            dbtable="creditcard_capstone.cdw_sapp_branch").load().toPandas()
print('Data read from table cdw_sapp_branch')

customer_cc_df = pd.merge(customer_df, cc_df, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')
branch_cc_df = pd.merge(branch_df, cc_df, how='inner', on='BRANCH_CODE')

def set_colors(num):
    return ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(num)]


categories = cc_df['TRANSACTION_TYPE'].value_counts()
transaction_colors = set_colors(len(categories))
categories.plot(kind='barh', figsize=(10, 5),
                xlim=(6000,7000),
                color=transaction_colors, 
                title='Total Transactions Per Category')
for index, value in enumerate(categories):
    plt.text(value+10, index-0.1, str(value))
plt.show()
print('Graph "Total Transactions Per Category" created')


states = customer_df['CUST_STATE'].value_counts()
state_colors = set_colors(len(states))
states.plot(kind='barh', figsize=(10, 5),
            color=state_colors, xlim=(0,100),
            title='Total Number of Customers Per State for 2018')
for index, value in enumerate(states):
    plt.text(value+0.6, index-0.4, str(value))
plt.show()
print('Graph "Total Number of Customers Per State for 2018" created')


highest_20 = customer_cc_df.groupby('CUST_SSN')['TRANSACTION_VALUE'] \
                           .sum() \
                           .sort_values(ascending=False) \
                           .head(20)
highest_colors = set_colors(len(highest_20))
highest_20.plot(kind='barh', figsize=(10, 5),
                xlim=(4800,5800),
                color=highest_colors, xlabel='Account Number',
                title='Top 20 Total Transaction Amounts Per Customer for 2018')
for index, value in enumerate(highest_20):
    plt.text(value+10, index-0.25, '$'+str(value))
plt.show()
print('Graph "Top 20 Total Transaction Amounts Per Customer for 2018" created')


top_3_months = cc_df['MONTH'].value_counts() \
                             .sort_values(ascending=False) \
                             .head(3)
top_colors = set_colors(len(top_3_months))
top_3_months.plot(kind='barh', figsize=(10, 5),
                  xlim=(3900, 3970),
                  color=top_colors, xlabel='Month',
                  title='Top 3 Total # of Transactions Per Month for 2018')
for index, value in enumerate(top_3_months):
    plt.text(value+1, index-0.04, str(value))
plt.show()
print('Graph "Top 3 Total # of Transactions Per Month for 2018" created')


branch_health = cc_df[cc_df['TRANSACTION_TYPE']=='Healthcare'].groupby('BRANCH_CODE', as_index=False)['TRANSACTION_VALUE'] \
                                                              .sum() \
                                                              .sort_values(by='TRANSACTION_VALUE', ascending=False)
branch_colors = set_colors(len(branch_health))
branch_health.plot(kind='scatter', x='BRANCH_CODE', y='TRANSACTION_VALUE',
                   figsize=(30, 10), color=branch_colors, xlabel='Branch #',
                   ylabel='Total Value of Healthcare Transactions', xlim=(0, 200),
                   title='Total $ Value of Healthcare Transactions Per Branch')
branch_health_series = branch_health.set_index('BRANCH_CODE').squeeze()
for index, value in branch_health_series.items():
    if value == branch_health_series.max(axis=0):
        plt.text(index-5, value-120, 'Highest Total', bbox=dict(facecolor='yellow',alpha=0.5))
    plt.text(index+1, value-20, '#'+str(index))

# alternate method for labeling the scatter plot
# for i in range(len(branch_health)):
#     plt.text(x=branch_health['BRANCH_CODE'][i]+1,
#              y=branch_health['TRANSACTION_VALUE'][i]+20,
#              s=branch_health['BRANCH_CODE'][i])

plt.show()
print('Graph "Total $ Value of Healthcare Transactions Per Branch" created')

spark.stop()
print('SparkSession ended')