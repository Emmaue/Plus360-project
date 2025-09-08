import requests

api_key = "009b851b587d30a94de3957fb116c8211a7af2b4"
COMPANYDOMAIN = "pqpmarketingconsult"
endpoint = f"https://{COMPANYDOMAIN}.pipedrive.com/api/v2/contacts/2"

params = {
    "api_token": api_key
}

response = requests.get(endpoint, params=params)

if response.status_code != 200:
    raise Exception(f"API call failed: {response.status_code}, {response.text}")

data = response.json()
print(data)
