import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import random

with open("./secret.txt") as f:
    secret_ls = f.readlines()
    user_name = secret_ls[0][:-1]
    user_password = secret_ls[1]

def set_colors(num):
    return ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(num)]

spark = SparkSession.builder.appName('Capstone').getOrCreate()
print('SparkSession created')

loan_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                          user=user_name,\
                                          password=user_password,\
                                          url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                          dbtable="creditcard_capstone.cdw_sapp_loan_application").load().toPandas()
print('Data read from table cdw_sapp_loan_application')

married_income = loan_df[['Application_Status','Gender','Income']]
married_income = married_income[(married_income['Application_Status']=='Y')].drop(['Application_Status'], axis=1)
married_income = married_income.groupby('Gender')['Income'].value_counts().unstack()
married_income = married_income.reindex(columns=['low','medium','high'])

bar_width = 0.9
num_of_bars = len(married_income.columns)
married_income.plot(kind='bar', figsize=(10, 5), ylabel='Application Approvals', width=bar_width,
                    title='Application Approvals of Married Applicants Based on Gender and Income Ranges')

spacing = -bar_width/num_of_bars
for income, counts in married_income.items():
    for index, count in enumerate(counts):
        plt.text(index+spacing, count+2, count, ha='center')
    spacing += bar_width/num_of_bars
    
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()
print('Graph "Application Approvals of Married Applicants Based on Gender and Income Ranges" created')

bar_width=0.5
prop_area = loan_df[['Application_Status','Property_Area']]
prop_area = prop_area.groupby('Application_Status')['Property_Area'].value_counts().unstack()
prop_area.plot(kind='barh', figsize=(10,5), width=bar_width,
               stacked=True, xlabel='Application Status',
               title='Application Approvals Based on Property Area')

tick_labels = ['Denied','Approved']
plt.yticks(ticks=range(len(tick_labels)), labels=tick_labels, rotation=0)
plt.tight_layout()
plt.show()
print('Graph "Application Approvals Based on Property Area" created')


approved = loan_df[loan_df['Application_Status']=='Y']
approved = approved.groupby(['Credit_History','Dependents','Education',
                            'Gender','Income','Married','Property_Area',
                            'Self_Employed'])['Application_Status'] \
                            .value_counts().rename('Number_of_Approvals') \
                            .to_frame().reset_index().drop('Application_Status',axis=1)

approved_index = []
for index, row in approved.iterrows():
    temp_string = ''
    for i, value in row.items():
        temp_string += str(value) + ','
    approved_index.append(temp_string[:-1])

approved = approved[['Number_of_Approvals']]
approved_index = pd.DataFrame(approved_index, columns=['Demographics'])
approved_results = pd.concat([approved_index, approved], axis=1).set_index('Demographics') \
                                                                .sort_values(by='Number_of_Approvals', ascending=True) \
                                                                .transpose()

approved_colors = set_colors(approved_results.size)
approved_results.plot(kind='barh', figsize=(15,8), color=approved_colors,
                      width=16, edgecolor='white', linewidth=1.5,
                      xlabel='Demographics', ylabel='Number of Approved Applications', 
                      title='Total # of Approved Applications Per Each Demographic')

plt.yticks(ticks=[])
plt.legend(fontsize='xx-small')
plt.tight_layout()
plt.show()
print('Graph "Total # of Approved Applications Per Each Demographic" created')

spark.stop()
print('SparkSession ended')