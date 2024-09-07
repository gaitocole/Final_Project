import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

def get_billboard_data(date):
	"""Scrape Billboard Hot 100 Data for a specific date"""
	url = f"https://www.billboard.com/charts/hot-100/{date}"
	response = requests.get(url)

	if response.status_code !=200:
		print(f"Failed to retrieve data for {date}")
		return[]

	soup = BeautifulSoup(response.content, 'html.parser')
	chart_data = []

	chart_entries = soup.find_all('li', class_='o-chart-results-list__item')

	for entry in chart_entries:
		rank_element = entry.find('span', class_='c-label')
		print(rank_element.prettyfy())
		rank = rank_element.strip() if entry.find('span', class_='c-label') else 'N/A'
		title_element = entry.find('h3', class_='c-title')
		title = title_element.text.strip() if title_element else 'N/A'
		artist_element = entry.find('span', class_='c-label')
		artist = artist_element.text.strip() if artist_element else 'N/A'

		chart_data.append([rank, title, artist])
	
	return chart_data


def scrape_billboard_data(start_date, end_date, output_file):
	"""Scrape Billboard Hot 100 data from start_date to end_date and write to Excel"""
	current_date = start_date
	all_data = []

	#Loop through each week
	while current_date <= end_date:
		date_str = current_date.strftime('%Y-%m-%d')
		print(f"Scraping data for {date_str}")

		#Get data for the current week
		weekly_data = get_billboard_data(date_str)

		if weekly_data:
			for row in weekly_data:
				row.insert(0, date_str) #Add the date to the column
			all_data.extend(weekly_data)

		#Move to the next week
		current_date += timedelta(days=7)

	#create a DataFrame and save to Excel
	df = pd.DataFrame(all_data, columns=['Date', 'Rank', 'Title', 'Artist'])
	df.to_excel(output_file, index=False)
	print(f"Data has been written to {output_file}")

if __name__ == "__main__":
	#Define the start and end date
	start_date_str = input("Enter the start date (YYYY-MM-DD): ")
	end_date_str = input("Enter the end date (YYYY-MM-DD): ")
	
	#convert input strings to datetime objects
	try:
		start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
		end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
	except ValueError:
		print("Error: Invalid date format. Please use YYYY-MM-DD.")
		exit(1) #Exit if the date format is invalid

	#specify output Excel file
	output_file = 'billboard_hot_100.xlsx'

	#scrape the data and write to excel
	scrape_billboard_data(start_date, end_date, output_file)
