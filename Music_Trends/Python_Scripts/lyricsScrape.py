import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging
import json
import time

def fetch_lyrics_from_url(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the div that contains the lyrics, using the correct class and attribute
            lyrics_div = soup.find('div', {'data-lyrics-container': 'true', 'class': 'Lyrics__Container-sc-1ynbvzw-1 kUgSbL'})
            
            # If found, extract the lyrics by getting all the text within this div
            if lyrics_div:
                lyrics = lyrics_div.get_text(separator="\n").strip()
                return lyrics
            else:
                print("Lyrics not found on this page.")
                return None
        else:
            print(f"Failed to fetch page, status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

# Define the file paths
file_path = 'hot-100-current-to-present_unique_items.xlsx'  # Original file
output_directory = os.path.dirname(file_path)  # Get the directory of the input file
partial_save_path = os.path.join(output_directory, 'hot-100-current-to-present_unique_items_lyrics_scrape_partial.xlsx')  # Partial save path
output_file = os.path.join(output_directory, 'hot-100-current-to-present_url_&_scrape_com.xlsx')  # Final output file

# Check if the partial save file exists
if os.path.exists(partial_save_path):
    # Load data from the partial save file
    df = pd.read_excel(partial_save_path)
    logging.info(f"Resuming from partial save: {partial_save_path}")
    print(f"Resuming from partial save: {partial_save_path}")
else:
    # Load the original data from original source file
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
        lyrics = fetch_lyrics_from_url(lyrics_url)
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
