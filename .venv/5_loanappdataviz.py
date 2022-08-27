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

married_income.plot(kind='bar', figsize=(10, 5), ylabel='Application Approvals',
                    title='Application Approvals of Married Applicants Based on Gender and Income Ranges')

for i in range(len(married_income)):
    plt.text(i-0.19,married_income['low'][i]+1.5,married_income['low'][i])
    plt.text(i-0.02,married_income['medium'][i]+1.5,married_income['medium'][i])
    plt.text(i+0.15,married_income['high'][i]+1.5,married_income['high'][i])
    
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

spark.stop()