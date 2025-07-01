import requests
import certifi

tableName = 'Periods'
modelName = 'Product Allocation Maximization'

api_key = 'uisfgkp0PKMsEuGP4ep2tp7MixM4wLDZ'
header = {'X-API-Key': api_key}
#api_url = 'https://api.llama.ai/v3/model'


api_url = f'https://api.llama.ai/v3/model/name/{modelName}/table/{tableName}'
response = requests.get(api_url, verify=False, headers=header)
print(response.json())
#response.json()
