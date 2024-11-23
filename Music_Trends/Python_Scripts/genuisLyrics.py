import os
import pandas as pd
import requests
import time
import logging
import json
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(filename='lyrics_fetch.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Load the API token from a JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

genius_token = config['GENUIS_API_TOKEN']

# Function to scrape lyrics from a Genius song URL
def scrape_lyrics(url):
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logging.error(f"Failed to fetch {url}, status code: {response.status_code}")
            return None
        
        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Genius lyrics are often inside <div> tags with class "Lyrics__Container"
        lyrics_divs = soup.find_all('div', class_='Lyrics__Container')
        
        if not lyrics_divs:
            logging.error(f"Lyrics not found on the page: {url}")
            return None
        
        # Extract text from all lyrics containers
        lyrics = '\n'.join([div.get_text(separator="\n").strip() for div in lyrics_divs])
        return lyrics

    except Exception as e:
        logging.error(f"Error occurred while fetching lyrics from {url}: {e}")
        return None

# Define a function to get song lyrics URL using the Genius API
def get_lyrics(song_title, artist=None, max_retries=3):
    # Base URL for Genius search
    base_url = "https://api.genius.com/search"
    
    # Set up headers with the API token
    headers = {
        "Authorization": f"Bearer {genius_token}"
    }
    
    # Build the query for the song (optional: include artist)
    query = f"{song_title} {artist}" if artist else song_title
    
    for attempt in range(max_retries):  # Retry up to max_retries times
        try:
            # Make the API request
            response = requests.get(base_url, headers=headers, params={'q': query})
            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()

                # If there's no result, return None
                if not data['response']['hits']:
                    logging.info(f"No lyrics found for {song_title} by {artist}")
                    return None
                
                # Get the first result's lyrics URL
                song_url = data['response']['hits'][0]['result']['url']
                
                # Log successful fetch
                logging.info(f"Lyrics URL found for {song_title} by {artist}: {song_url}")
                
                return song_url
            
            else:
                logging.warning(f"Failed request for {song_title} by {artist}, Status code: {response.status_code}")
                return None

        except requests.exceptions.ConnectionError as e:
            logging.error(f"ConnectionError for {song_title} by {artist}: {e}, attempt {attempt + 1}")
            time.sleep(2)  # Wait 2 seconds before retrying
            
        except Exception as e:
            logging.error(f"Unexpected error for {song_title} by {artist}: {e}")
            return None

    logging.error(f"Failed to retrieve data for {song_title} by {artist} after {max_retries} attempts")
    return None

# Define the file paths
file_path = 'Blank_Data.xlsx'  # Original file
output_directory = os.path.dirname(file_path)  # Get the directory of the input file
partial_save_path = os.path.join(output_directory, 'Blank_Data_partial_save.xlsx')  # Partial save path
output_file = os.path.join(output_directory, 'Blank_Data_final_output.xlsx')  # Final output file

# Check if the partial save file exists
if os.path.exists(partial_save_path):
    # Load data from the partial save file
    df = pd.read_excel(partial_save_path)
    logging.info(f"Resuming from partial save: {partial_save_path}")
    print(f"Resuming from partial save: {partial_save_path}")
else:
    # Load the original data from charts.xlsx
    df = pd.read_excel(file_path)
    logging.info(f"Starting fresh from {file_path}")
    print(f"Starting fresh from {file_path}")

# Check if an existing output file is present
if os.path.exists(output_file):
    # Read the existing data
    existing_df = pd.read_excel(output_file)
    # Merge the dataframes on the 'Song' column to retain previously fetched lyrics and URLs
    df = df.merge(existing_df[['Song', 'Lyrics_URL', 'Lyrics']], on='Song', how='left', suffixes=('', '_existing'))
    # Drop duplicates in case some rows are already processed
    df.drop(columns=['Lyrics_existing'], errors='ignore', inplace=True)
else:
    # Ensure that 'Lyrics_URL' and 'Lyrics' columns exist, and initialize them if not
    if 'Lyrics_URL' not in df.columns:
        df['Lyrics_URL'] = ''  # Initialize with empty strings if the column doesn't exist
    if 'Lyrics' not in df.columns:
        df['Lyrics'] = ''  # Initialize with empty strings if the column doesn't exist

# Ensure the columns are of string type to avoid dtype issues
df['Lyrics_URL'] = df['Lyrics_URL'].astype(str)
df['Lyrics'] = df['Lyrics'].astype(str)

# Create lists to store the lyrics URLs and lyrics content
lyrics_urls = df['Lyrics_URL'].tolist()
lyrics_content = df['Lyrics'].tolist()

for index, row in df.iterrows():
    song = row['Song']
    artist = row['Artist'] if 'Artist' in df.columns else None
    
    # Initialize lyrics_url
    lyrics_url = None

    # If the lyrics URL is missing, fetch it
    if pd.isna(row['Lyrics_URL']) or row['Lyrics_URL'] == 'nan' or row['Lyrics_URL'] == '':
        lyrics_url = get_lyrics(song, artist)
        lyrics_urls[index] = str(lyrics_url)
        df.at[index, 'Lyrics_URL'] = lyrics_url  # Save the URL immediately
        
        # Save the partial progress including the URL
        df.to_excel(partial_save_path, index=False)
    else:
        # Use existing URL from the row
        lyrics_url = row['Lyrics_URL']

    # If the lyrics are missing but we have a URL, scrape the lyrics from the URL
    if (pd.isna(row['Lyrics']) or row['Lyrics'] == 'nan' or row['Lyrics'] == '') and lyrics_url not in [None, 'nan', '']:
        lyrics = scrape_lyrics(lyrics_url)
        lyrics_content[index] = str(lyrics)
        df.at[index, 'Lyrics'] = lyrics  # Save the lyrics immediately
        
        # Save the partial progress including the lyrics
        df.to_excel(partial_save_path, index=False)
    
    # Output song title once it's processed
    print(f"Processed: {song} by {artist}")
    logging.info(f"Processed: {song} by {artist}")
    
    # Save the dataframe periodically to prevent data loss
    if index % 10 == 0:  # Save after every 10 rows
        df['Lyrics_URL'] = pd.Series(lyrics_urls)
        df['Lyrics'] = pd.Series(lyrics_content)
        df.to_excel(partial_save_path, index=False)
        logging.info(f"Partial save after processing {index + 1} songs.")

# After the loop, ensure the full dataframe is saved
df['Lyrics_URL'] = pd.Series(lyrics_urls)
df['Lyrics'] = pd.Series(lyrics_content)
df.to_excel(output_file, index=False)

print(f"Lyrics have been fetched and saved to '{output_file}'.")
logging.info(f"Lyrics fetching process completed and saved to '{output_file}'.")
