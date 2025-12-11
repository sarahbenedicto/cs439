#Code not needed since not using Philly anymore

import requests

#No API Key we are making our own
APIKEY = "yes" #you have to make your own
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

url = "https://api.yelp.com/v3/businesses/search"

params = {
    "location": "Manhattan, NY",
    "term": "restaurant",
    "limit": 1
}

r = requests.get(url, headers=HEADERS, params=params)

print("Status:", r.status_code)
print("Response:", r.text)
