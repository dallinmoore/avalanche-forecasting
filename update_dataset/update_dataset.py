import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
import cv2
import numpy as np
import time
from read_the_rose import read_the_rose, coordinates

# Define the URL
base_url = 'https://utahavalanchecenter.org'
archives_url = '/archives/forecasts?page='

def process_rose_data(input_file, output_file, get_rose_link_function):
    try:
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames + list(coordinates.keys())  # Add danger level columns
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            infile.seek(0)  # Reset file pointer
            next(reader)  # Skip header
            total_rows = sum(1 for _ in reader)  # Count total number of rows
            infile.seek(0)  # Reset file pointer again
            next(reader)  # Skip header again

            start_time = time.time()
            for i, row in enumerate(reader, start=1):
                link = row['Link']
                rose_link = get_rose_link_function(base_url + link)

                if rose_link:
                    try:
                        response = requests.get(rose_link)
                        response.raise_for_status()  # Raise an exception for HTTP errors
                        image_data = response.content
                        # Decode the image from memory
                        image_np = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
                        rose_data = read_the_rose(image_np)
                        row.update(rose_data)
                    except Exception as e:
                        print(f"Error processing rose image for {link}: {e}")  # Log the error

                # Calculate and print progress percentage
                percent_complete = (i / total_rows) * 100
                elapsed_time = time.time() - start_time
                print(f"Progress: {percent_complete:.2f}% complete. {elapsed_time//60:.0f} min {elapsed_time%60:.2f} sec elapsed.", end='\r', flush=True)

                writer.writerow(row)
        print("\nCSV write complete.")
    except Exception as e:
        print(f"Error in process_rose_data: {e}")

def get_rose_link(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.text, 'html.parser')
        rose_img = soup.find('img', class_="full-width compass-width sm-pb3")
        return rose_img['src'] if rose_img else None
    except Exception as e:
        print(f"Error occurred while processing {url}: {e}")  # Log the error
        return None

def scrape_forecast_data(latest_date):
    page = 0
    url = base_url + archives_url + str(page)
    data = []

    while True:
        print(f'Scraping page {page}.', end='\r')
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check if the page has no results
        if soup.find('div', class_='view-empty'):
            print("No results found. Stopping loop.")
            break

        # Find all the table rows
        table_rows = soup.find_all('tr')

        # Iterate through each row
        for row in table_rows[1:]:  # Skip the first row as it contains headers
            columns = row.find_all('td')
            date_issued = columns[0].get_text().strip()
            forecast_area = columns[1].get_text()[11:-15].strip()
            link = columns[1].find('a')['href']  # Extract link from the second column

            # Append the data as a dictionary to the list
            data.append({'Date Issued': date_issued,
                         'Forecast Area': forecast_area,
                         'Link': link})

            # Check if the current date matches the latest date in the dataset
            current_date = pd.to_datetime(date_issued)
            if current_date <= latest_date:
                return pd.DataFrame(data)

        # Increment page number for the next iteration
        page += 1
        url = base_url + archives_url + str(page)

# Main execution
if __name__ == "__main__":
    try:
        curr_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the script
        input_csv = os.path.join(curr_dir, 'updated-avalanche-forecast-links.csv')
        output_csv = os.path.join(curr_dir, 'avalanche-forecast-rose-new.csv')

        # Load the dataset to determine the latest date
        data = pd.read_csv('avalanche-forecast-rose.csv')
        latest_date = pd.to_datetime(data['date']).max()

        # Scrape new forecast data
        new_data_df = scrape_forecast_data(latest_date)

        # Save the scraped data to a CSV file
        new_data_df.to_csv(input_csv, index=False)

        # Process the rose data
        process_rose_data(input_csv, output_csv, get_rose_link)
    except Exception as e:
        print(f"Error in main execution: {e}")
