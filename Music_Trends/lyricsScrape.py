import requests
from bs4 import BeautifulSoup

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

# Example usage
url = 'https://genius.com/Billie-eilish-8-lyrics'  # replace with the actual URL
lyrics = fetch_lyrics_from_url(url)
if lyrics:
    print(lyrics)
