import requests

url = 'http://192.168.26.78:5000/absen_wt'
syntax_query = 'SELECT * FROM absst'

data = {'query': syntax_query}
response = requests.post(url, json=data)

print(response.json())