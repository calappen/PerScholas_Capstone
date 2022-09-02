# Per Scholas Capstone
#### Final project for the Per Scholas Data Engineering course

## Project requirements:

### 1. Load Credit Card Database (SQL)
#### 1.1) Data Extraction and Transformation with Python and PySpark
For “Credit Card System,” create a Python and PySpark SQL program to read/extract the following JSON files according to the specifications found in the mapping document.
- CDW_SAPP_BRANCH.JSON
- CDW_SAPP_CREDITCARD.JSON
- CDW_SAPP_CUSTOMER.JSON
#### 1.2) Data Loading Into Database
Once PySpark reads data from JSON files, and then utilizes Python, PySpark, and Python modules to load data into RDBMS(SQL), perform the following:
- Create a Database in SQL(MariaDB), named “creditcard_capstone.”
- Create a Python and Pyspark Program to load/write the “Credit Card System Data” into RDBMS(creditcard_capstone)
	- Tables should be created by the following names in RDBMS:
		- CDW_SAPP_BRANCH
		- CDW_SAPP_CREDIT_CARD
		- CDW_SAPP_CUSTOMER

### 2. Application Front-End
Once data is loaded into the database, we need a front-end (console) to see/display data. For that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2).
#### 2.1) Transaction Details Module
- Display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
- Display the number and total values of transactions for a given type.
- Display the number and total values of transactions for branches in a given state.
#### 2.2) Customer Details Module
- Check the existing account details of a customer.
- Modify the existing account details of a customer.
- Generate a monthly bill for a credit card number for a given month and year.
- Display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

### 3. Credit Card Data Analysis and Visualization
After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data according to the below requirements. Use Python libraries for the below requirements:
#### 3.1) Graph Transaction Types
- Show which transaction type occurs most often.
#### 3.2) Graph States
- Show which state has the highest number of customers.
#### 3.3) Graph Transaction Amounts
- Show the sum of all transactions for each customer, and which customer has the highest transaction amount. (First 20)
#### 3.4) Graph Months
- Show the top three months with the largest transaction data.
#### 3.5) Graph Branch Transactions
- Show each branch's healthcare transactions, seeing which branch processed the highest total dollar value of healthcare transactions.

### Overview of LOAN application Data API
Banks deal in all home loans. They have a presence across all urban, semi-urban, and rural areas. Customers first apply for a home loan; after that, a company will validate the customer's eligibility for a loan.

Banks want to automate the loan eligibility process (in real-time) based on customer details provided while filling out the online application form. These details are Gender, Marital Status, Education, Number of Dependents, Income, Loan Amount, Credit History, and others. To automate this process, they have the task of identifying the customer segments to those who are eligible for loan amounts so that they can specifically target these customers. Here they have provided a partial dataset.

#### API Endpoint: [https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json](https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json)

The above URL allows you to access information about loan application information. This dataset has all of the required fields for a loan application. You can access data from a REST API by sending an HTTP request and processing the response.

### 4. LOAN Application Dataset
#### 4.1) Access API Endpoint
- Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
#### 4.2) Access Status Code
- Find the status code of the above API endpoint.
#### 4.3) Load Into Database
- Once Python reads data from the API, utilize PySpark to load data into RDBMS(SQL). The table name should be "CDW-SAPP_loan_application" in the database. Use the “creditcard_capstone” database.

### 5. Loan Data Analysis and Visualization
#### 5.1) Graph Income Ranges
- Create a bar chart that shows the difference in application approvals for Married Men vs Married Women based on income ranges. (number of approvals)
#### 5.2) Graph Property Area Based Application Approvals/Denials
- Create and plot a chart that shows the difference in application approvals based on Property Area.
#### 5.3) Graph Demographic Based Approved Applications
- Create a multi-bar plot that shows the total number of approved applications per each application demographic.

## Graph Results:
### 3.1) Transaction Types
![Credit Card Transaction Types](/.venv/Graphs/3_1.png)
### 3.2) States
![States](/.venv/Graphs/3_2.png)
### 3.3) Transaction Amounts
![Transaction Amounts](/.venv/Graphs/3_3.png)
### 3.4) Months
![Months](/.venv/Graphs/3_4.png)
### 3.5) Branch Transactions
![Branch Transactions](/.venv/Graphs/3_5.png)
### 5.1) Income Ranges
![Income Ranges](/.venv/Graphs/5_1.png)
### 5.2) Property Area Based Application Approvals/Denials
![Property Area Based Application Approvals/Denials](/.venv/Graphs/5_2.png)
### 5.3) Demographic Based Approved Applications
![Demographic Based Approved Applications](/.venv/Graphs/5_3.png)

## Technical Challenges
### Transforming Data With PySpark Dataframes
The data in section 1 and 4 were imported as a dataframe using PySpark. The data then needed to edited and transformed before being sent to the SQL database using PySpark. PySpark dataframes are more challenging to work with when transforming data. The solution was taking the PySpark dataframes and converting them to Pandas dataframes, where Pandas has more user-friendly functionality with transforming data in dataframes. Once editing and transforming was complete, the Pandas dataframes were then converted back to PySpark to be sent to the database.
### Modifying Customer Account Details
One project requirement in section 2.2 is having the option to modify a customer's account details. This includes every part of a customer's account excluding a few parameters ('SSN' and 'LAST_UPDATED' columns). The 'FULL_STREET_ADDRESS' was excluded as well since there are options to update the apartment number and street name individually. Each time either of these details were modified the 'FULL_STREET_ADDRESS' column was updated. Overall, the option to modify customer account details was challenging as it was it was the most time-consuming portion of the project. The options individually needed to be coded in the console menu. Each option though was written as much as possible in a way that the code could apply to either option. Writing it this way allowed the first option's code to be copied and repeated for the other options, with some edits to each option's coding. Although this portion of the project took the longest to implement, writing the code this way did shorten the time it took to complete it.
### Labeling Values with Pandas Plot
The Pandas package comes with Matplotlib's graphing functionality and was used to graph the data in section 3 and 5. The package doesn't easily allow for the ability to label points or bars with their values though. This took some research, testing, and time to implement. In the future, the Plotly package may be used instead as it offers a mouseover functionality which could be used to display a point or bar's value when hovering over it.
### Demographic Multi-bar Graph
The graph for section 5.3 was challenging as it is difficult to clearly display each demographic. Each parameter of a demographic was combined into a comma-separated string and used to display each demographic in the legend of the graph. Because of the lengthly list of demographics, the legend extends down past the graph, not properly displaying the full list of demographics. The graph's size was increased and the legend's text was decreased to allow a better view of the legend but some demographics are still being cut off. In the future, the Plotly package would be explored for its mouseover feature and be used to resolve this issue. The demographic string could be displayed when hovering over that demographc's bar in the graph and the legend would not be needed.

## Skillsets
- Python
- SQL
- Apache Spark
- REST API
- Git
- MariaDB
- Pandas library
- Matplotlib library