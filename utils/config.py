import os
from dotenv import load_dotenv

load_dotenv()

# endpoints and urls
token_url = "https://accounts.spotify.com/api/token"
base_rul = "https://api.spotify.com/v1"
new_releases_endpoint = "browse/new-releases"
logging_folder = 'logs'


# primary_paths
album_search_load_path = '/data/album_search'
DATABASE_PATH = os.getenv('DATA_BASE_PATH', 'data')
LANDING_ZONE_PATH = f'{DATABASE_PATH}/landing_zone'
BRONZE_PATH = f'{DATABASE_PATH}/bronze'
SILVER_PATH = f'{DATABASE_PATH}/silver'

# silver layer paths
SILVER_FACT_ALBUMS = "fact_albums"
SILVER_DIM_ARTISTS = "dim_artists"
SILVER_DIM_IMAGES = "dim_images"
SILVER_BRIDGE_ARTISTS_ALBUMS = "bridge_artists_albums"
SILVER_BRIDGE_IMAGES_ALBUMS = "bridge_images_albums"

GOLD_PATH = f'{DATABASE_PATH}/gold'
GOLD_ARTISTS_SUMMARY = "gold_artists"
GOLD_ARTISTS_GROWTH = "gold_artist_growth"
