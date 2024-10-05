import requests
import pandas as pd

def search_itunes(song_title, artist_name, limit=1):
    """Search iTunes for a song and artist and return the metadata"""
    base_url = "https://itunes.apple.com/search"
    search_params = {
        "term": f"{song_title} {artist_name}",
        "media": "music",
        "entity": "song",
        "limit": limit
    }

    response = requests.get(base_url, params=search_params)

    if response.status_code != 200:
        print("Failed to retrieve data from iTunes")
        return None
    
    data = response.json()
    if 'results' in data and len(data['results']) > 0:
        return data['results'][0] #Return the first result (song metadata)
    else:
        return None
    
def fetch_metadata(input_file, output_file):
    """Fetch iTunes metadata for songs in the Excel file"""

    #Read the Excel file
    df = pd.read_excel(input_file)

    #Add new columns to store metadata
    df['Album'] = ""
    df['Genre'] = ""
    df['Release Date'] = ""
    df['iTunes URL'] = ""

    for index, row in df.iterrows():
        title = row['song']
        artist = row['artist']

        #Search iTunes for the song
        itunes_data = search_itunes(title, artist)

        if itunes_data:
            df.at[index, 'Album'] = itunes_data.get('collectionName', "")
            df.at[index, 'Genre'] = itunes_data.get('primaryGenreName', "")
            df.at[index, 'Release Date'] = itunes_data.get('releaseDate', "")
            df.at[index, 'iTunes URL'] = itunes_data.get('trackViewUrl', "")
        else:
            df.at[index, 'Album'] = "N/A"
            df.at[index, 'Genre'] = "N/A"
            df.at[index, 'Release Date'] = "N/A"
            df.at[index, 'iTunes URL'] = "N/A"
        
    df.to_excel(output_file, index=False)
    print(f"Metadata has been written to {output_file}")

if __name__ == "__main__":
    input_file = input("Enter the input Excel file: ")
    output_file = input("Enter the output Excel file: ")

    fetch_metadata(input_file, output_file)