import requests
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

api_url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
response = requests.get(api_url)
loan_data = response.json()

print('API endpoint status code: ' + str(response.status_code))

spark = SparkSession.builder.appName('Capstone').getOrCreate()

loan_data_df = spark.createDataFrame(loan_data)

loan_data_df.write.format("jdbc") \
 .mode("append") \
 .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
 .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
 .option("user", "root") \
 .option("password", "Pass1234") \
 .save()

spark.stop()