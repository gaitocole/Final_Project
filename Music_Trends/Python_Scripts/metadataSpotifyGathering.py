import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import json
import time
import logging
from spotipy.exceptions import SpotifyException
import requests

# Set up logging
logging.basicConfig(filename='spotify_metadata.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the filename for reading and writing the Excel file
FILENAME = 'Blank_Data.xlsx'

# Load the Spotify credentials from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

client_id = config['spotify']['client_id']
client_secret = config['spotify']['client_secret']

# Set up Spotify credentials
client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager, requests_timeout=10)

# Load your song dataset (use pd.read_excel to load as a DataFrame)
df = pd.read_excel(FILENAME)

# Add columns for additional metadata
df['Track Popularity'] = None
df['Track Explicit'] = None
df['Album'] = None
df['Album Release Date'] = None
df['Artist Popularity'] = None
df['Artist Genres'] = None
df['Track ID'] = None
df['Total Tracks in Album'] = None

MAX_RETRIES = 3

# Function to fetch metadata from Spotify with retry logic
def fetch_metadata(row):
    if pd.isnull(row['Title']) or pd.isnull(row['Artist']):
        logging.warning(f"Skipping row due to missing Title or Artist: {row}")
        return row

    retries = 0
    while retries < MAX_RETRIES:
        try:
            logging.info(f"Fetching metadata for {row['Title']} by {row['Artist']}")
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

        except SpotifyException as e:
            if e.http_status == 429:  # Rate limit exceeded
                retry_after = int(e.headers.get('Retry-After', 10))  # default to 10 seconds
                print(f"Rate limit exceeded. Retrying in {retry_after} seconds...")
                logging.warning(f"Rate limit exceeded. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)
            else:
                logging.error(f"Error fetching data for {row['Title']} by {row['Artist']}: {e}")
                return row

        except requests.exceptions.Timeout:
            logging.error(f"Timeout while fetching data for {row['Title']} by {row['Artist']}")
            return row

        except requests.exceptions.RequestException as e:
            logging.error(f"Network or API error for {row['Title']} by {row['Artist']}: {e}")
            return row

        except Exception as e:
            retries += 1
            logging.error(f"Retry {retries}/{MAX_RETRIES} failed for {row['Title']} by {row['Artist']}: {e}")
            if retries == MAX_RETRIES:
                logging.error(f"Max retries reached for {row['Title']} by {row['Artist']}. Skipping.")
                return row
            time.sleep(2)  # Wait before retrying

    return row

# Function to save DataFrame to a new Excel file with "_new" appended to the filename
def save_to_new_excel(data, filename):
    new_filename = filename.replace('.xlsx', '_new.xlsx')
    try:
        # Try to append to the new Excel file
        with pd.ExcelWriter(new_filename, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            data.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
    except FileNotFoundError:
        # If the file does not exist, create it with headers
        with pd.ExcelWriter(new_filename, mode='w', engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name='Sheet1', index=False)

# Main function to fetch metadata for all songs and save to a new Excel file
def fetch_all_metadata(df, filename):
    # Start from the first row
    start_index = 0
    print(f"Starting from row {start_index}")
    logging.info(f"Starting from row {start_index}")

    updated_rows = []
    for index, row in df.iterrows():
        # Check if any of the metadata columns are missing (None or NaN)
        if pd.isnull(row['Track Popularity']) or pd.isnull(row['Track Explicit']) or pd.isnull(row['Album']) or pd.isnull(row['Album Release Date']) or pd.isnull(row['Artist Popularity']) or pd.isnull(row['Artist Genres']) or pd.isnull(row['Track ID']) or pd.isnull(row['Total Tracks in Album']):
            
            print(f"Missing metadata for row {index}: {row['Title']} by {row['Artist']}. Fetching...")
            logging.info(f"Missing metadata for row {index}: {row['Title']} by {row['Artist']}. Fetching...")
            updated_row = fetch_metadata(row)  # Fetch metadata if any value is missing
            updated_rows.append(updated_row)

        else:
            print(f"Row {index}: {row['Title']} by {row['Artist']} already has metadata. Skipping...")
            logging.info(f"Row {index}: {row['Title']} by {row['Artist']} already has metadata. Skipping...")

        # Save every 10 rows to the new Excel file
        if (index + 1) % 10 == 0:
            temp_df = pd.DataFrame(updated_rows)
            save_to_new_excel(temp_df, filename)
            print(f"Saved rows {index-9} to {index} to new file: {filename.replace('.xlsx', '_new.xlsx')}")
            logging.info(f"Saved rows {index-9} to {index} to new file: {filename.replace('.xlsx', '_new.xlsx')}")
            updated_rows = []  # Clear the buffer after saving

        # Add delay between requests to avoid rate limits
        time.sleep(1)  # 1-second delay between requests

    # Save any remaining rows
    if updated_rows:
        temp_df = pd.DataFrame(updated_rows)
        save_to_new_excel(temp_df, filename)
        print(f"Saved remaining rows to new file: {filename.replace('.xlsx', '_new.xlsx')}")
        logging.info(f"Saved remaining rows to new file: {filename.replace('.xlsx', '_new.xlsx')}")

# Call the function to fetch metadata and save it
fetch_all_metadata(df, FILENAME)
