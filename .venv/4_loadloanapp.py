import requests
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import mysql.connector

with open("./secret.txt") as f:
    secret_ls = f.readlines()
    user_name = secret_ls[0][:-1]
    user_password = secret_ls[1]

mydb = mysql.connector.connect(
  host="localhost",
  user=user_name,
  password=user_password,
  database="creditcard_capstone"
)
print('Connected to DB')

mycursor = mydb.cursor()

api_url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
response = requests.get(api_url)
loan_data = response.json()
print('Data pulled from API')

print('API endpoint status code: ' + str(response.status_code))

spark = SparkSession.builder.appName('Capstone').getOrCreate()
print('SparkSession created')

loan_data_df = spark.createDataFrame(loan_data)

mycursor.execute("""DROP TABLE IF EXISTS CDW_SAPP_loan_application""")

loan_data_df.write.format("jdbc") \
                  .mode("append") \
                  .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
                  .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
                  .option("user", user_name) \
                  .option("password", user_password) \
                  .save()
print('Data written to table CDW_SAPP_loan_application')

spark.stop()
print('SparkSession ended')

mycursor.close()
mydb.close()