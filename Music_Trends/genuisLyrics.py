import pandas as pd
import requests
import time
import logging

# Setup logging
logging.basicConfig(filename='lyrics_fetch.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Define a function to get song lyrics using the Genius API
def get_lyrics(song_title, artist=None, max_retries=3):
    # Add your Genius API token here
    genuis_token = 'YOUR_GENIUS_API_TOKEN'
    
    # Base URL for Genius search
    base_url = "https://api.genius.com/search"
    
    # Set up headers with the API token
    headers = {
        "Authorization": f"Bearer {genuis_token}"
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

# Read the Excel file (assuming it has a 'Song' column, and optionally an 'Artist' column)
df = pd.read_excel('charts.xlsx')

# Create a new column 'Lyrics_URL' to store the lyrics or URL
lyrics_urls = []

for index, row in df.iterrows():
    song = row['Song']
    artist = row['Artist'] if 'Artist' in df.columns else None
    lyrics_url = get_lyrics(song, artist)
    
    # Append the URL or None (if not found) to the list
    lyrics_urls.append(lyrics_url)
    
    # Save the dataframe periodically to prevent data loss
    if index % 10 == 0:  # Save after every 10 rows
        df['Lyrics_URL'] = pd.Series(lyrics_urls)
        df.to_excel('songs_with_lyrics_partial.xlsx', index=False)
        logging.info(f"Partial save after processing {index + 1} songs.")

# After the loop, ensure the full dataframe is saved
df['Lyrics_URL'] = pd.Series(lyrics_urls)
df.to_excel('songs_with_lyrics.xlsx', index=False)

print("Lyrics have been fetched and saved to 'songs_with_lyrics.xlsx'.")
logging.info("Lyrics fetching process completed and saved to 'songs_with_lyrics.xlsx'.")
