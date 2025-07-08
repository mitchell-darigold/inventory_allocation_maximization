import requests
import certifi

modelId = 'Product Allocation Maximization'
tableName = 'Periods'
modelName = 'Product Allocation Maximization'
macroId = 'b8b0c083-cccc-4e2e-a504-6ae4ec0a9462'

api_key = 'uisfgkp0PKMsEuGP4ep2tp7MixM4wLDZ'
header = {'X-API-Key': api_key}

#api_url = 'https://api.llama.ai/v3/model'
#api_url = f'https://api.llama.ai/v3/model/name/{modelName}/table/{tableName}'
#api_url = f'https://api.llama.ai/v3/model/{modelId}/table/{tableName}/record'
api_url = f'https://api.llama.ai/v3/macro/{macroId}/execution'

#response = requests.get(api_url, verify=False, headers=header)
response = requests.post(api_url, verify=False, headers=header)
print(response.json())
#response.json()
