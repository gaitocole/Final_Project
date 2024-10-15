import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import aiohttp
import asyncio
import json
from openpyxl import load_workbook

# Load the Spotify credentials from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

client_id = config['spotify']['client_id']
client_secret = config['spotify']['client_secret']

# Set up Spotify credentials
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Load your song dataset (replace 'your_song_data.csv' with your actual file)
df = pd.read_csv('your_song_data.csv')

# Add columns for additional metadata
df['Track Popularity'] = None
df['Track Explicit'] = None
df['Album'] = None
df['Album Release Date'] = None
df['Artist Popularity'] = None
df['Artist Genres'] = None
df['Playlist Followers'] = None
df['Playlist Name'] = None

# Function to fetch metadata from Spotify asynchronously
async def fetch_metadata(session, row):
    try:
        # Search for the track by title and artist
        result = sp.search(q=f'track:{row["Title"]} artist:{row["Artist"]}', type='track')
        if result['tracks']['items']:
            track = result['tracks']['items'][0]
            
            # Fetch Track Metadata
            row['Track Popularity'] = track['popularity']
            row['Track Explicit'] = track['explicit']
            row['Album'] = track['album']['name']
            row['Album Release Date'] = track['album']['release_date']
            row['Track ID'] = track['id']
            row['Total Tracks in Album'] = track['album']['total_tracks']
            
            # Fetch Artist Metadata
            artist_id = track['artists'][0]['id']
            artist = sp.artist(artist_id)
            row['Artist Popularity'] = artist['popularity']
            row['Artist Genres'] = ', '.join(artist['genres'])
            
            # Fetch Playlist Metadata (dummy for now as actual playlist might differ)
            row['Playlist Followers'] = None  # Add this if you're dealing with actual playlist data
            row['Playlist Name'] = None  # Replace with real playlist metadata if needed
            
        return row
    except Exception as e:
        print(f"Error fetching data for {row['Title']} by {row['Artist']}: {e}")
        return row

# Asynchronous driver function to fetch metadata for all songs
async def fetch_all_metadata(df, filename):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in df.iterrows():
            tasks.append(fetch_metadata(session, row))
        
        # Fetch the metadata asynchronously
        updated_rows = await asyncio.gather(*tasks)
        
        # Update the DataFrame
        updated_df = pd.DataFrame(updated_rows)
        
        # Save every 10 rows to the Excel file
        for i in range(0, len(updated_df), 10):
            save_to_excel(updated_df.iloc[i:i+10], filename)
            print(f"Saved rows {i} to {i+9} to {filename}")

# Function to save rows to Excel
def save_to_excel(data, filename):
    try:
        # Try to append to the existing Excel file
        with pd.ExcelWriter(filename, mode='a', engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name='Sheet1', index=False, header=writer.sheets['Sheet1'] is None)
    except FileNotFoundError:
        # If the file does not exist, create it
        data.to_excel(filename, sheet_name='Sheet1', index=False)

# Start the async process to fetch metadata and save to Excel
loop = asyncio.get_event_loop()
loop.run_until_complete(fetch_all_metadata(df, 'song_metadata.xlsx'))
