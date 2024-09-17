import pandas as pd
import requests

#Define a func to get song lyric using the Genuis API
def get_lyrics(song_title, artist=None):
        #add your Genuis API token here
    genuis_token = 'Mt0kn1HuJqAZ4tPNudWn3pOcY5hUqBOzw-JbQYqrsOs-G5EgZnT2_0LjUESjZ52Z'

        #base_url for Genuis search
    base_url = "https://api.genuis.com/search"

        #set up deaders with the API token
    headers = {
        "Authorization":f"Bearer {genuis_token}"
        }

        #Build the query for the song (optional: include artist)
    query = f"{song_title} {artist}" if artist else song_title

    response = requests.get(base_url, headers=headers, params={'q':query})

    if response.status_code != 200:
        return None  # Return None if the API request failed
        
        data = response.json()
        
        # If there's no result, return None
    if not data['response']['hits']:
        return None
        
            # Get the first result's lyrics URL
    song_url = data['response']['hits'][0]['result']['url']
        
            # For simplicity, return the song's Genius URL (can also scrape lyrics from the page if needed)
    return song_url

# Read the Excel file (assuming it has a 'Song' column, and optionally an 'Artist' column)
df = pd.read_excel('charts.xlsx')

# Create a new column 'Lyrics_URL' to store the lyrics or URL
df['Lyrics_URL'] = df.apply(lambda row: get_lyrics(row['Song'], row['Artist'] if 'Artist' in df.columns else None), axis=1)

# Save the updated dataframe back to the Excel file
df.to_excel('songs_with_lyrics.xlsx', index=False)

print("Lyrics have been fetched and saved to 'songs_with_lyrics.xlsx'.")
