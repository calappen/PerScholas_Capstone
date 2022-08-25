import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np

# pd.set_option('max_columns', None)

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

branch_states = pd.unique(branch_df['BRANCH_STATE'])
transaction_types = pd.unique(cc_df['TRANSACTION_TYPE'])

# print('Gas' in transaction_types)

# zip_code = '93035'
# day = '25'
# month = '11'

# print(df_result.head())

option = ''
run_main_menu = True
run_t_menu = ''
run_c_menu = ''
zip_code = ''
month = ''
day = ''
t_type = ''
b_state = ''


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
                    zip_code = input('\nEnter ZIP code: ')
                    zip_code = zip_code.strip()
                    if zip_code.isdigit() and len(zip_code) == 5:
                        print('Valid ZIP code')
                        break
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    month = input('\nEnter month using 2 digits: ')
                    month = month.strip()
                    if month.isdigit and len(month) == 2 and month != '00':
                        if int(month.lstrip()) in range(0,13):
                            print('Valid month')
                            break
                        else:
                            print('\nInvalid month. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                while True:
                    day = input('\nEnter day using 2 digits: ')
                    day = day.strip()
                    if day.isdigit and len(day) == 2 and day != '00':
                        if int(day.lstrip()) in range(0,32):
                            print('Valid day\n')
                            break
                        else:
                            print('\nInvalid day. Try again.')
                    else:
                        print('\nInvalid entry. Try again.')
                
                df_result = customer_cc_df[(customer_cc_df['CUST_ZIP'] == zip_code) & 
                                        (customer_cc_df['DAY'] == day) & 
                                        (customer_cc_df['MONTH'] == month)]

                if df_result.empty == True:
                    print('\nNo data matching criteria.')
                else:
                    print(df_result.sort_values(by='DAY', ascending=False))
                    print('*sorted by day descending*')

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
                        print('\nValid transaction type')
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
                    if b_state.isalpha() == True and len(b_state) == 2:
                        if b_state.upper() in branch_states:
                            print('\nBranch(es) found for given state\n')
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
                print('\nResult1')
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
                print('\nResult2')
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
                print('\nResult3')
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
                print('\nResult4')
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