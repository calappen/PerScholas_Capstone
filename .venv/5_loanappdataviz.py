import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Capstone').getOrCreate()

loan_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                     user="root",\
                                     password="Pass1234",\
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                     dbtable="creditcard_capstone.cdw_sapp_loan_application").load().toPandas()


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

spark.stop()