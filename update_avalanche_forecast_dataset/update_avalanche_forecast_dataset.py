import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import csv
import time
import json
import numpy as np
from PIL import Image

class AvalancheForecastScraper:
    BASE_URL = 'https://utahavalanchecenter.org'
    ARCHIVES_URL = '/archives/forecasts?page='
    
    def __init__(self):
        self.curr_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Load coordinates and danger levels from JSON files
        with open(os.path.join(self.curr_dir, 'coordinates.json'), 'r') as coord_file:
            self.coordinates = json.load(coord_file)['coordinates']

        with open(os.path.join(self.curr_dir, 'colors.json'), 'r') as colors_file:
            self.danger_levels = {int(k): v for k, v in json.load(colors_file)['danger_levels'].items()}
    
    def get_rose_link(self, url):
        """Extract the rose image URL from a forecast page."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            rose_img = soup.find('img', class_="full-width compass-width sm-pb3")
            return rose_img['src'] if rose_img else None
        except Exception as e:
            print(f"Error extracting rose link from {url}: {e}")
            return None
    
    def read_the_rose(self, image):
        """
        Extract danger levels from the rose image.
        Expects a PIL Image object.
        """
        # Convert the image to HSV color space
        hsv_image = image.convert("HSV")
        hsv_array = np.array(hsv_image)

        # Dictionary to store danger levels for each point
        danger_levels = {}

        # Loop through each coordinate in the coordinates dictionary
        for point, coord in self.coordinates.items():
            # Extract the HSV value at the coordinate
            hsv_value = hsv_array[coord[1], coord[0]]

            # Convert HSV value to danger level
            danger_level = self.convert_to_danger_level(hsv_value)
            
            # Store the danger level in the dictionary
            danger_levels[point] = danger_level

        return danger_levels
    
    def convert_to_danger_level(self, hsv_value):
        """Convert HSV value to danger level based on the closest match."""
        # Calculate Euclidean distance between the HSV value and each danger level
        distances = {level: np.linalg.norm(np.array(hsv_value) - np.array(level_hsv)) 
                     for level, level_hsv in self.danger_levels.items()}
        
        # Find the danger level with the minimum distance
        closest_level = min(distances, key=distances.get)
        
        return closest_level
    
    def process_rose_data(self, input_file, output_file):
        """Process the rose data from forecast images and add to CSV."""
        try:
            with open(input_file, 'r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                fieldnames = reader.fieldnames + list(self.coordinates.keys())
                
                # Count total rows for progress tracking
                infile.seek(0)
                next(reader)  # Skip header
                total_rows = sum(1 for _ in reader)
                
                # Reset file pointer
                infile.seek(0)
                rows = list(reader)
            
            # Process rows and collect results
            results = []
            start_time = time.time()
            
            for i, row in enumerate(rows, start=1):
                link = row['Link']
                
                # Check if the link already includes the base URL
                if not link.startswith('http'):
                    rose_link = self.get_rose_link(self.BASE_URL + link)
                else:
                    rose_link = self.get_rose_link(link)
                
                if rose_link:
                    try:
                        # Check if rose_link is a relative URL and prepend BASE_URL if needed
                        if rose_link.startswith('/'):
                            rose_link = self.BASE_URL + rose_link
                        
                        # Download image data into memory buffer first
                        response = requests.get(rose_link)
                        response.raise_for_status()
                        image_data = response.content
                        
                        # Create PIL image from memory buffer
                        from io import BytesIO
                        pil_image = Image.open(BytesIO(image_data))
                        
                        # Process the image to extract danger levels
                        rose_data = self.read_the_rose(pil_image)
                        row.update(rose_data)
                    except Exception as e:
                        print(f"Error processing rose image for {link}: {e}")
                
                results.append(row)
                
                # Update progress
                self._update_progress(i, total_rows, start_time)
            
            # Write results to output file
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
                
            print("\nCSV write complete.")
            
        except Exception as e:
            print(f"Error in process_rose_data: {e}")
    
    def scrape_forecast_data(self, latest_date=None):
        """Scrape forecast data up to the latest_date."""
        page = 0
        data = []
        
        while True:
            url = f"{self.BASE_URL}{self.ARCHIVES_URL}{page}"
            print(f'Scraping page {page}', end='\r')
            
            try:
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if the page has no results
                if soup.find('div', class_='view-empty'):
                    print("\nNo more results found. Stopping.")
                    break
                
                # Extract data from table rows
                table_rows = soup.find_all('tr')
                
                for row in table_rows[1:]:  # Skip header row
                    columns = row.find_all('td')
                    date_issued = columns[0].get_text().strip()
                    forecast_area = columns[1].get_text()[11:-15].strip()
                    link = columns[1].find('a')['href']
                    
                    # Ensure the link has the full URL
                    if not link.startswith('http'):
                        link = self.BASE_URL + link
                    
                    data.append({
                        'Date Issued': date_issued,
                        'Forecast Area': forecast_area,
                        'Link': link
                    })
                    
                    # Stop if we've reached the latest date
                    if latest_date and pd.to_datetime(date_issued) <= latest_date:
                        return pd.DataFrame(data)
                
                page += 1
                
            except Exception as e:
                print(f"\nError scraping page {page}: {e}")
                break
        
        return pd.DataFrame(data)
    
    def _update_progress(self, current, total, start_time):
        """Display progress information."""
        percent_complete = (current / total) * 100
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = elapsed_time % 60
        print(f"Progress: {percent_complete:.2f}% complete. {minutes} min {seconds:.2f} sec elapsed.", end='\r', flush=True)
    
    def run(self):
        """Main execution function."""
        try:
            # Define main output file path
            output_csv = os.path.join(self.curr_dir, '..', 'avalanche-forecast-rose.csv')
            
            # Load existing data to find latest date
            try:
                existing_data = pd.read_csv(output_csv)
                latest_date = pd.to_datetime(existing_data['Date Issued']).max()
                print(f"Getting forecasts newer than {latest_date}")
            except (FileNotFoundError, KeyError):
                latest_date = None
                existing_data = pd.DataFrame()
                print("No existing data found, scraping all available forecasts")
            
            # Scrape new forecast data
            new_data_df = self.scrape_forecast_data(latest_date)
            
            if new_data_df.empty:
                print("No new forecasts found.")
                return
                
            print(f"Found {len(new_data_df)} new forecasts to process")
            
            # Process rows and collect results
            results = []
            start_time = time.time()
            
            for i, row in enumerate(new_data_df.to_dict('records'), start=1):
                link = row['Link']
                
                # Check if the link already includes the base URL
                if not link.startswith('http'):
                    rose_link = self.get_rose_link(self.BASE_URL + link)
                else:
                    rose_link = self.get_rose_link(link)
                
                if rose_link:
                    try:
                        # Check if rose_link is a relative URL and prepend BASE_URL if needed
                        if rose_link.startswith('/'):
                            rose_link = self.BASE_URL + rose_link
                        
                        # Download image data into memory buffer first
                        response = requests.get(rose_link)
                        response.raise_for_status()
                        image_data = response.content
                        
                        # Create PIL image from memory buffer
                        from io import BytesIO
                        pil_image = Image.open(BytesIO(image_data))
                        
                        # Process the image to extract danger levels
                        rose_data = self.read_the_rose(pil_image)
                        row.update(rose_data)
                    except Exception as e:
                        print(f"Error processing rose image for {link}: {e}")
                
                results.append(row)
                
                # Update progress
                self._update_progress(i, len(new_data_df), start_time)
            
            # Create a DataFrame with all processed results
            results_df = pd.DataFrame(results)
            
            # Write results to temporary file first
            temp_output = os.path.join(self.curr_dir, '..', 'temp_avalanche-forecast-rose.csv')
            
            if not os.path.exists(output_csv):
                # Create new file if doesn't exist
                results_df.to_csv(temp_output, index=False)
            else:
                # Combine with existing data and save to temp file
                combined_df = pd.concat([existing_data, results_df], ignore_index=True)
                combined_df.to_csv(temp_output, index=False)
            
            # Only after successful write to temp file, rename to actual output file
            if os.path.exists(temp_output):
                if os.path.exists(output_csv):
                    os.replace(temp_output, output_csv)  # Atomic operation
                else:
                    os.rename(temp_output, output_csv)
                
            print(f"\nAdded {len(results)} new forecasts to {output_csv}")
            
        except Exception as e:
            print(f"Error in execution: {e}")

    def test_rose_reading(self, image_path):
        """Test function to read a rose from a local image file."""
        try:
            image = Image.open(image_path)
            danger_levels = self.read_the_rose(image)
            print(f"Danger levels extracted from {os.path.basename(image_path)}:")
            for point, level in danger_levels.items():
                print(f"  {point}: {level}")
            return danger_levels
        except Exception as e:
            print(f"Error testing rose reading: {e}")
            return None

if __name__ == "__main__":
    scraper = AvalancheForecastScraper()
    
    # Check if there's a command line argument for testing
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        if len(sys.argv) > 2:
            # Test with provided image path
            scraper.test_rose_reading(sys.argv[2])
        else:
            # Test with default image
            test_image = os.path.join(scraper.curr_dir, 'practice.png')
            scraper.test_rose_reading(test_image)
    else:
        # Run the main scraper
        scraper.run()