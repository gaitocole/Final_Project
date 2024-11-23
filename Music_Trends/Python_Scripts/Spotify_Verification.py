import json
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy

# Load the Spotify credentials from the config.json file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

client_id = config['spotify']['client_id_5']
client_secret = config['spotify']['client_secret_5']

# Check if credentials are loaded correctly
if not client_id or not client_secret:
    raise Exception("Spotify API credentials are missing in the config.json file.")

# Set up Spotify credentials manager
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

# Initialize Spotify client with credentials manager
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Test the connection by fetching a song to ensure the API works
result = sp.search(q='track:Hello artist:Adele', type='track')
print(result)
