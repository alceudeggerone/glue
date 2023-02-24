import requests
import yaml
import json
url = 'https://raw.githubusercontent.com/alceudeggerone/glue/master/yamlteste/teste.yaml'

payload={}

response = requests.request("GET", url, verify=False)

a=response.content
data=yaml.safe_load(a)

print(data)




