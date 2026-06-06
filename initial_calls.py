import requests as re
import base64
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import json


load_dotenv()


client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

token_url = "https://accounts.spotify.com/api/token"
endpoint = "albums"
id = "4aawyAB9vmqN3uQ7FjRGTy"


credentials = f"{client_id}:{client_secret}"
client_creds_b64 = base64.b64encode(credentials.encode('ascii')).decode()
# print(client_creds_b64)

token_data = {
    "grant_type": "client_credentials"
}

token_headers = {
    "Authorization": f"Basic {client_creds_b64}"
}

print(token_headers)

req = re.post(token_url, data=token_data, headers=token_headers)
token_response = req.json()

print(req.status_code)

access_token = token_response['access_token']
access_token_expiray = token_response['expires_in']

# print(token_response)
# curl --request GET \
#     'https://api.spotify.com/v1/tracks/2TpxZ7JUBn3uw46aR7qd6V' \
#      --header "Authorization: Bearer BQBT31EGs6q-YpAjzpjcNNHfi2Dmvc_Azrb4b6wmXivJlmXmSp-7wiUL8-n6tnyMKl2oQUcHkTAJoyIfi1xTdS_RGTgJKEL_1m1RNtXIkfrt_vm04o-dE0VRQp1lHqTeVBgGixQAfQY"

base_url = "https://api.spotify.com/v1"
endpoint = "albums"
id = "4aawyAB9vmqN3uQ7FjRGTy"

headers = {
    "Authorization": f"Bearer {access_token}"
}


try:
    sample_album = re.get(url=f"{base_url}/{endpoint}/{id}", headers=headers)
    print(sample_album.json())
except:
    print(f"{base_url}/{endpoint}/{id}")


artist_followers = re.get('https://api.spotify.com/v1/artists/0TnOYISbd1XYRBk9myaseg',
                          headers=headers).json()

print(artist_followers.get('followers', {}).get('total', {}))
print(artist_followers.get('popularity', {}))


# start from the latest released albums
last_7_days = (datetime.now() - timedelta(days=7)).date().isoformat()
results = []

new_album_releases = re.get(
    'https://api.spotify.com/v1/browse/new-releases',
    headers=headers,
    params={'limit': 20, 'country': 'US'}
)
new_album_releases.raise_for_status()

counter = 50
url = 'https://api.spotify.com/v1/browse/new-releases'

while url:
    response = re.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    for album in data.get('albums',{}).get('items', []):
        print(f"{album['name']} — {album['release_date']}")
        if album['release_date'] >= last_7_days:
            results.append(album)
    url = data.get('albums',{}).get('next')
    print(url)



print(len(results))

with open(f'/Users/sajad/Documents/GitHub/spotify_databricks/data/new_albums_{last_7_days}.json', 'w') as output_file:
    json.dump(results, output_file, indent=2)
