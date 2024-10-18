import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
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

# Load your song dataset (use pd.read_excel to load as a DataFrame)
df = pd.read_excel('Unique_Songs_Lyrics_Spotify.xlsx')

# Add columns for additional metadata
df['Track Popularity'] = None
df['Track Explicit'] = None
df['Album'] = None
df['Album Release Date'] = None
df['Artist Popularity'] = None
df['Artist Genres'] = None
df['Track ID'] = None
df['Total Tracks in Album'] = None

# Function to fetch metadata from Spotify
def fetch_metadata(row):
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
        
        return row
    except Exception as e:
        print(f"Error fetching data for {row['Title']} by {row['Artist']}: {e}")
        return row

# Function to save DataFrame to Excel
def save_to_excel(data, filename):
    try:
        # Try to append to the existing Excel file
        with pd.ExcelWriter(filename, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            data.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
    except FileNotFoundError:
        # If the file does not exist, create it
        data.to_excel(filename, sheet_name='Sheet1', index=False)

# Main function to fetch metadata for all songs and save to Excel
def fetch_all_metadata(df, filename):
    try:
        # Load the existing file to find how many rows have already been processed
        existing_df = pd.read_excel(filename)
        start_index = len(existing_df)
        print(f"Resuming from row {start_index}")
    except FileNotFoundError:
        # If the file doesn't exist, start from the beginning
        start_index = 0

    updated_rows = []
    for index, row in df.iloc[start_index:].iterrows():
        updated_row = fetch_metadata(row)
        updated_rows.append(updated_row)
        
        # Save every 10 rows to the Excel file
        if (index + 1) % 10 == 0:
            temp_df = pd.DataFrame(updated_rows)
            save_to_excel(temp_df, filename)
            print(f"Saved rows {index-9} to {index} to {filename}")
            updated_rows = []  # Clear the buffer after saving
    
    # Save any remaining rows
    if updated_rows:
        temp_df = pd.DataFrame(updated_rows)
        save_to_excel(temp_df, filename)
        print(f"Saved remaining rows to {filename}")

# Call the function to fetch metadata and save it
fetch_all_metadata(df, 'song_metadata.xlsx')
