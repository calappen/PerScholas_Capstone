import requests

api_url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
response = requests.get(api_url)
loan_data = response.json()

print('API endpoint status code: ' + str(response.status_code))