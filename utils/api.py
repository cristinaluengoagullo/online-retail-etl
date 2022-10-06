import requests
from config.config import conf

# Generic api request
def get_api_data(url, params):
    response = requests.get(url, params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Request not successful. Code: " + str(response.status_code))
        exit()
