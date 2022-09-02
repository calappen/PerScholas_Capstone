import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import mysql.connector
import pandas as pd
import re

pd.set_option('max_columns', None)

with open("./secret.txt") as f:
    secret_ls = f.readlines()
    user_name = secret_ls[0][:-1]
    user_password = secret_ls[1]

spark = SparkSession.builder.appName('Capstone').getOrCreate()
print('SparkSession created')

mydb = mysql.connector.connect(
  host="localhost",
  user=user_name,
  password=user_password,
  database="creditcard_capstone"
)
print('Connected to DB')

mycursor = mydb.cursor()

customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                              user=user_name,\
                                              password=user_password,\
                                              url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                              dbtable="creditcard_capstone.cdw_sapp_customer").load().toPandas()
print('Data read from table cdw_sapp_customer')

customer_df['SSN'] = customer_df['SSN'].astype(str)

customer_columns = customer_df.columns.tolist()
customer_columns.remove('LAST_UPDATED')
customer_columns.remove('SSN')
customer_columns.remove('FULL_STREET_ADDRESS')

cc_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                        user=user_name,\
                                        password=user_password,\
                                        url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                        dbtable="creditcard_capstone.cdw_sapp_credit_card").load().toPandas()
print('Data read from table cdw_sapp_credit_card')

cc_df['DAY'] = cc_df['DAY'].str.lstrip('0').astype(int)
cc_df['MONTH'] = cc_df['MONTH'].str.lstrip('0').astype(int)
cc_df['TIMEID'] = pd.to_datetime(cc_df['TIMEID'])

branch_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                            user=user_name,\
                                            password=user_password,\
                                            url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                            dbtable="creditcard_capstone.cdw_sapp_branch").load().toPandas()
print('Data read from table cdw_sapp_branch')

customer_cc_df = pd.merge(customer_df, cc_df, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')
branch_cc_df = pd.merge(branch_df, cc_df, how='inner', on='BRANCH_CODE')

branch_states = pd.unique(branch_df['BRANCH_STATE'])
transaction_types = pd.unique(cc_df['TRANSACTION_TYPE'])

main_menu = ('\n'
             '1) Transaction Details\n'
             '2) Customer Details\n'
             '3) Quit\n')
transaction_menu = ('\n'
                    '1) Transactions made by customers living in a given zip code for a given month and year\n'
                    '2) Number and total values of transactions for a given type\n'
                    '3) Number and total values of transactions for branches in a given state\n'
                    '4) Previous menu\n'
                    '5) Quit\n')
customer_menu = ('\n'
                 '1) Check the existing account details of a customer\n'
                 '2) Modify the existing account details of a customer\n'
                 '3) Generate a monthly bill for a credit card number for a given month and year\n'
                 '4) Transactions made by a customer between two dates\n'
                 '5) Previous menu\n'
                 '6) Quit\n')
return_quit_t = ('\n'
               '1) Return to main menu\n'
               '2) Return to transaction menu\n'
               '3) Quit\n')
return_quit_c = ('\n'
               '1) Return to main menu\n'
               '2) Return to customer menu\n'
               '3) Quit\n')

def valid_email(email):
    pattern = '^[a-zA-Z0-9-_]+@[a-zA-Z0-9]+\.[a-z]{1,3}$'
    if re.match(pattern, email):
        return True
    else:
        return False

run_main_menu = True

while run_main_menu:
    run_t_menu = True
    run_c_menu = True

    print(main_menu)
    option = input('Enter option: ')
    option = option.strip()

    # Transaction menu
    if option == '1':
        print(transaction_menu)
        while run_t_menu:
            option = input('Enter option: ')
            option = option.strip()

            if option == '1':
                while True:
                    zip_code = input('\nEnter ZIP code using 5 digits: ')
                    zip_code = zip_code.strip()
                    if zip_code.isdigit() and len(zip_code) == 5:
                        print('Valid ZIP code')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    month = input('\nEnter month using 2 digits: ')
                    month = month.strip().lstrip('0')
                    if month.isdigit():
                        if int(month) in range(0,13):
                            print('Valid month')
                            break
                        else:
                            print('\nInvalid month. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    year = input('\nEnter year using 4 digits: ')
                    year = year.strip()
                    if year.isdigit() and len(year) == 4:
                        print('Valid day\n')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                
                df_result = customer_cc_df[(customer_cc_df['CUST_ZIP'] == zip_code) & 
                                           (customer_cc_df['YEAR'] == int(year)) & 
                                           (customer_cc_df['MONTH'] == int(month))]

                if df_result.empty:
                    print('\nNo data matching criteria.')
                else:
                    print(df_result.sort_values(by='DAY', ascending=False))
                    print('*ordered by day descending*')

                while True:
                    print(return_quit_t)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_t_menu = False
                        break
                    elif option == '2':
                        print(transaction_menu)
                        break
                    elif option == '3':
                        run_t_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '2':
                while True:
                    t_type = input('\nEnter transaction type: ')
                    t_type = t_type.strip()
                    if t_type.title() in transaction_types:
                        print('\nValid transaction type\n')
                        break
                    else:
                        print('\nNo transaction type found. Try again.')

                t_count = cc_df[(cc_df['TRANSACTION_TYPE'] == t_type.title())].shape[0]
                t_value = cc_df[(cc_df['TRANSACTION_TYPE'] == t_type.title())]['TRANSACTION_VALUE'].sum(axis=0)
                print('\nNumber of transactions for ' + t_type.lower() + ": " + str(t_count))
                print('Total value of transactions for ' + t_type.lower() + ": $" + str(t_value))

                while True:
                    print(return_quit_t)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_t_menu = False
                        break
                    elif option == '2':
                        print(transaction_menu)
                        break
                    elif option == '3':
                        run_t_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '3':
                while True:
                    b_state = input('\nEnter state using 2 letters: ')
                    b_state = b_state.strip()
                    if b_state.isalpha() and len(b_state) == 2:
                        if b_state.upper() in branch_states:
                            print('\nBranch(es) found for given state')
                            break
                        else:
                            print('\nNo branches found. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')

                b_count = branch_cc_df[(branch_cc_df['BRANCH_STATE'] == b_state.upper())].shape[0]
                b_value = branch_cc_df[(branch_cc_df['BRANCH_STATE'] == b_state.upper())]['TRANSACTION_VALUE'].sum(axis=0)
                print('\nNumber of transactions for branches in ' + b_state.upper() + ": " + str(b_count))
                print('Total value of transactions for branches in ' + b_state.upper() + ": $" + str(b_value))

                while True:
                    print(return_quit_t)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_t_menu = False
                        break
                    elif option == '2':
                        print(transaction_menu)
                        break
                    elif option == '3':
                        run_t_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '4':
                break

            elif option == '5':
                run_main_menu = False
                break

            else:
                print('\nInvalid option. Try again.')
                print(transaction_menu)

    # Customer menu
    elif option == '2':
        print(customer_menu)
        while run_c_menu:
            option = input('Enter option: ')
            option = option.strip()

            if option == '1':
                while True:
                    ssn = input('\nEnter SSN for customer using 9 digits: ')
                    ssn = ssn.replace('-','').replace(' ','')
                    if ssn.isdigit() and len(ssn) == 9:
                        print('Valid SSN\n')
                        break
                    else:
                        print('\nInvalid entry. Try again.')

                customer_df=spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",\
                                                              user=user_name,\
                                                              password=user_password,\
                                                              url="jdbc:mysql://localhost:3306/creditcard_capstone",\
                                                              dbtable="creditcard_capstone.cdw_sapp_customer").load().toPandas()
                
                customer_df['SSN'] = customer_df['SSN'].astype(str)
                customer_cc_df = pd.merge(customer_df, cc_df, how='inner', left_on='CREDIT_CARD_NO', right_on='CUST_CC_NO')
                customer_acct_details = customer_df[(customer_df['SSN'] == ssn)]
                print(customer_acct_details)

                while True:
                    print(return_quit_c)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_c_menu = False
                        break
                    elif option == '2':
                        print(customer_menu)
                        break
                    elif option == '3':
                        run_c_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '2':
                while True:
                    ssn = input('\nEnter SSN for customer using 9 digits: ')
                    ssn = ssn.replace('-','').replace(' ','')
                    if ssn.isdigit() and len(ssn) == 9:
                        print('Valid SSN\n')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                
                count = 1
                for c in customer_columns:
                    if count < 10:
                        print(' ' + str(count) + ') ' + c)
                    else:
                        print(str(count) + ') ' + c)
                    count += 1
                print(str(count) + ') Exit options')
                
                while True:
                    print('\nWhat account detail would you like to modify?')
                    option = input('Enter option: ')
                    option = option.strip()
                    if option.isdigit():
                        if int(option) <= len(customer_columns):
                            column_name = customer_columns[int(option)-1]
                            if option == '1': # APT_NO
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ' using digits: ')
                                    new_entry = new_entry.strip()
                                    if new_entry.isdigit():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET APT_NO=%s, FULL_STREET_ADDRESS=%s
                                                        WHERE SSN=%s;
                                                        """
                                        new_full_address = new_entry + ',' + customer_df[(customer_df['SSN'] == ssn)]['STREET_NAME'].values[0]
                                        variables = (new_entry, new_full_address, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        print('FULL_STREET_ADDRESS updated.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '2': # CREDIT_CARD_NO
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ' using 16 digits: ')
                                    new_entry = new_entry.strip()
                                    if new_entry.isdigit() and len(new_entry) == 16:
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CREDIT_CARD_NO=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '3': # CUST_CITY
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ': ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_CITY=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Use only letters. Try again.')

                            elif option == '4': # CUST_COUNTRY
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ': ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_COUNTRY=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Use only letters. Try again.')

                            elif option == '5': # CUST_EMAIL
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + '. Follow format name@example.com: ')
                                    new_entry = new_entry.strip()
                                    if valid_email(new_entry):
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_EMAIL=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid email. Try again.')

                            elif option == '6': # CUST_PHONE
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ' using 7 or 10 digits: ')
                                    new_entry = new_entry.strip()
                                    if new_entry.isdigit() and len(new_entry) == 7:
                                        new_entry = '000' + new_entry
                                    if new_entry.isdigit() and len(new_entry) == 10:
                                        new_entry = '(' + new_entry[:3] + ')' + new_entry[3:6] + '-' + new_entry[6:]
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_PHONE=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '7': # CUST_STATE
                                while True:
                                    new_entry = input('\nEnter new ' + column_name + ' using 2 letters: ')
                                    new_entry = new_entry.strip().upper()
                                    if new_entry.isalpha() and len(new_entry) == 2:
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_STATE=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '8': # CUST_ZIP
                                while True:
                                    new_entry = input('\nEnter new ' + column_name + ' using 5 digits: ')
                                    new_entry = new_entry.strip()
                                    if new_entry.isdigit() and len(new_entry) == 5:
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET CUST_ZIP=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '9': # FIRST_NAME
                                while True:
                                    new_entry = input('\nEnter new ' + column_name + ' using letters: ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.replace(' ','').replace('-','').isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET FIRST_NAME=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '10': # LAST_NAME
                                while True:
                                    new_entry = input('\nEnter new ' + column_name + ' using letters: ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.replace(' ','').replace('-','').isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET LAST_NAME=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            elif option == '11': # MIDDLE_NAME
                                while True:
                                    new_entry = input('\nEnter new ' + column_name + ' using letters: ')
                                    new_entry = new_entry.strip().lower()
                                    if new_entry.replace(' ','').replace('-','').isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET MIDDLE_NAME=%s
                                                        WHERE SSN=%s;
                                                        """
                                        variables = (new_entry, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                            if option == '12': # STREET_NAME
                                while True: 
                                    new_entry = input('\nEnter new ' + column_name + ' using letters: ')
                                    new_entry = new_entry.strip().title()
                                    if new_entry.isalpha():
                                        update_query = """
                                                        UPDATE cdw_sapp_customer
                                                        SET STREET_NAME=%s, FULL_STREET_ADDRESS=%s
                                                        WHERE SSN=%s;
                                                        """
                                        new_full_address = customer_df[(customer_df['SSN'] == ssn)]['APT_NO'].values[0] + ',' + new_entry
                                        variables = (new_entry, new_full_address, ssn)
                                        mycursor.execute(update_query,variables,)
                                        mydb.commit()
                                        print(column_name + ' modified.')
                                        print('FULL_STREET_ADDRESS updated.')
                                        break
                                    else:
                                        print('\nInvalid entry. Try again.')

                        elif int(option) == count:
                            break
                        else:
                            print('\nInvalid option. Try again.')
                    else:
                        print('\nInvalid option. Try again.')

                while True:
                    print(return_quit_c)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_c_menu = False
                        break
                    elif option == '2':
                        print(customer_menu)
                        break
                    elif option == '3':
                        run_c_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '3':
                while True:
                    cc_number = input('\nEnter credit card number using 16 digits: ')
                    cc_number = cc_number.strip()
                    if cc_number.isdigit() and len(cc_number) == 16:
                        print('Valid credit card number')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    month = input('\nEnter month using 2 digits: ')
                    month = month.strip().lstrip('0')
                    if month.isdigit():
                        if int(month) in range(0,13):
                            print('Valid month')
                            break
                        else:
                            print('\nInvalid month. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    year = input('\nEnter year using 4 digits: ')
                    year = year.strip()
                    if year.isdigit() and len(year) == 4:
                        print('Valid year\n')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                
                monthly_bill = cc_df[(cc_df['CUST_CC_NO'] == cc_number) & 
                                     (cc_df['MONTH'] == int(month)) & 
                                     (cc_df['YEAR'] == int(year))]['TRANSACTION_VALUE'].sum()

                if monthly_bill == 0.0:
                    print('No data matching criteria.')
                else:
                    print('Total for credit card ' + cc_number + ' in ' + month + '/' + year + ': $'+ str(monthly_bill))

                while True:
                    print(return_quit_c)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_c_menu = False
                        break
                    elif option == '2':
                        print(customer_menu)
                        break
                    elif option == '3':
                        run_c_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '4':
                while True:
                    ssn = input('\nEnter SSN for customer using 9 digits: ')
                    ssn = ssn.replace('-','').replace(' ','')
                    if ssn.isdigit() and len(ssn) == 9:
                        print('Valid SSN')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    initial_day = input('\nEnter initial day using 2 digits: ')
                    initial_day = initial_day.strip()
                    if initial_day.isdigit() and len(initial_day) == 2 and initial_day != '00':
                        if int(initial_day.lstrip('0')) in range(0,32):
                            print('Valid day')
                            break
                        else:
                            print('\nInvalid day. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    initial_month = input('\nEnter initial month using 2 digits: ')
                    initial_month = initial_month.strip()
                    if initial_month.isdigit() and len(initial_month) == 2 and initial_month != '00':
                        if int(initial_month.lstrip('0')) in range(0,13):
                            print('Valid month')
                            break
                        else:
                            print('\nInvalid month. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    initial_year = input('\nEnter initial year using 4 digits: ')
                    initial_year = initial_year.strip()
                    if initial_year.isdigit() and len(initial_year) == 4:
                        print('Valid year')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    final_day = input('\nEnter final day using 2 digits: ')
                    final_day = final_day.strip()
                    if final_day.isdigit() and len(final_day) == 2 and final_day != '00':
                        if int(final_day.lstrip('0')) in range(0,32):
                            print('Valid day')
                            break
                        else:
                            print('\nInvalid day. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    final_month = input('\nEnter final month using 2 digits: ')
                    final_month = final_month.strip()
                    if final_month.isdigit() and len(final_month) == 2 and final_month != '00':
                        if int(final_month.lstrip('0')) in range(0,13):
                            print('Valid month')
                            break
                        else:
                            print('\nInvalid month. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    final_year = input('\nEnter final year using 4 digits: ')
                    final_year = final_year.strip()
                    if final_year.isdigit() and len(final_year) == 4:
                        print('Valid year')
                        break
                    else:
                        print('\nInvalid entry. Try again.')

                start_date = initial_year + '-' + initial_month + '-' + initial_day
                end_date = final_year + '-' + final_month + '-' + final_day
                transactions = customer_cc_df[(customer_cc_df['SSN'] == ssn) & 
                                              (customer_cc_df['TIMEID'] >= start_date) &
                                              (customer_cc_df['TIMEID'] <= end_date )]

                if transactions.empty:
                    print('\nNo data matching criteria.')
                else:
                    print(transactions.sort_values(by=['YEAR', 'MONTH', 'DAY'], ascending=False))
                    print('*ordered by year, month, day descending*')

                while True:
                    print(return_quit_c)
                    option = input('Enter option: ')
                    option = option.strip()
                    if option == '1':
                        run_c_menu = False
                        break
                    elif option == '2':
                        print(customer_menu)
                        break
                    elif option == '3':
                        run_c_menu = False
                        run_main_menu = False
                        break
                    else:
                        print('\nInvalid option. Try again.')

            elif option == '5':
                break

            elif option == '6':
                run_main_menu = False
                break

            else:
                print('\nInvalid option. Try again.')
                print(customer_menu)

    # Quit menu
    elif option == '3':
        break

    else:
        print('\nInvalid option. Try again.')


spark.stop()
print('SparkSession ended')

mycursor.close()
mydb.close()