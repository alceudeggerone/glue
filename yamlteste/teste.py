import requests
import yaml
import json
url = 'https://raw.githubusercontent.com/alceudeggerone/glue/master/yamlteste/teste.yaml'

payload={}

response = requests.request("GET", url, verify=False)

a=response.content
data=yaml.safe_load(a)
teste_json=json.dumps(data)
print(teste_json)
print(data['sql'])

responsesql = requests.request("GET", data['sql'], verify=False)

sql=responsesql.content
data_sql=yaml.safe_load(sql)
print(data_sql['sql'])




