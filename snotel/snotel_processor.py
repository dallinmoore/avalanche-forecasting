import requests
import csv
import pandas as pd
import json
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import os
import argparse
from datetime import datetime, date, timedelta

BASE_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/"

def fetch_snotel_metadata():
    """Fetch SNOTEL station metadata from the AWDB REST API"""
    print("Fetching SNOTEL metadata...")
    metadata_url = BASE_URL + "stations"
    params = {
        "stationTriplets": "*:UT:SNTL",
        "returnForecastPointMetadata": "false",
        "returnReservoirMetadata": "false",
        "returnStationElements": "false",
        "activeOnly": "true",
        "durations": "HOURLY"
    }
    response = requests.get(metadata_url, params=params)
    if response.ok:
        data = response.json()
        save_to_csv(data)
        return True
    else:
        print("Request failed with status code:", response.status_code)
        return False

def save_to_csv(data):
    """Save SNOTEL station metadata to CSV file"""
    csv_path = os.path.join(os.path.dirname(__file__), "snotel_data.csv")
    keys = ["stationId", "stateCode", "networkCode", "name", "countyName", "elevation", "latitude", "longitude", "beginDate"]
    
    # Add date column to indicate last update
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys + ["date"])
        writer.writeheader()
        for station in data:
            row_data = {key: station.get(key, "") for key in keys}
            row_data["date"] = current_date
            writer.writerow(row_data)
    print(f"Saved {len(data)} SNOTEL stations to {csv_path}")

def fetch_snotel_data(start_date=None, end_date=None):
    """Fetch SNOTEL data for a specific date range"""
    print(f"Fetching SNOTEL data from {start_date} to {end_date}...")
    
    # If both start_date and end_date are provided, fetch data for that range
    # Otherwise, just fetch the metadata (default behavior)
    if start_date and end_date:
        try:
            # Here you would implement the logic to fetch SNOTEL data for a specific date range
            # For now, we'll just fetch metadata as a placeholder
            print(f"Note: Date range functionality ({start_date} to {end_date}) not yet implemented")
            return fetch_snotel_metadata()
        except Exception as e:
            print(f"Error fetching SNOTEL data for date range: {e}")
            return False
    else:
        return fetch_snotel_metadata()

def load_region_boundaries():
    """Load region boundaries from JSON file"""
    try:
        boundaries_path = os.path.join(os.path.dirname(__file__), "region_boundaries.json")
        with open(boundaries_path, "r") as f:
            data = json.load(f)
            return data["regions"]
    except Exception as e:
        print(f"Error loading region boundaries: {e}")
        return {}

def determine_region(lat, lon, region_shapes):
    """Determine which region a point belongs to based on lat/lon"""
    point = Point(lat, lon)
    
    for region, polygon in region_shapes.items():
        if polygon.contains(point):
            return region
    
    # If no exact match, find the closest region
    min_distance = float('inf')
    closest_region = "Unknown"
    
    for region, polygon in region_shapes.items():
        distance = polygon.exterior.distance(point)
        if distance < min_distance:
            min_distance = distance
            closest_region = region
    
    return closest_region

def map_stations_to_regions():
    """Map SNOTEL stations to avalanche forecast regions"""
    print("Mapping stations to regions...")
    
    # Check if CSV exists first
    csv_path = os.path.join(os.path.dirname(__file__), "snotel_data.csv")
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Run fetch_snotel_data first.")
        return False
    
    # Load region boundaries
    region_polygons = load_region_boundaries()
    if not region_polygons:
        print("Error: Could not load region boundaries.")
        return False
    
    # Convert polygon coordinates to shapely Polygon objects
    region_shapes = {region: Polygon(coords) for region, coords in region_polygons.items()}
    
    # Read SNOTEL data
    df = pd.read_csv(csv_path)
    
    # Map each station to a region
    df['region'] = df.apply(lambda row: determine_region(row['latitude'], row['longitude'], region_shapes), axis=1)
    
    # Save the updated data back to CSV
    df.to_csv(csv_path, index=False)
    
    # Group stations by region
    region_stations = {}
    for region in region_polygons.keys():
        region_df = df[df['region'] == region]
        region_stations[region] = region_df[['stationId', 'name', 'elevation', 'latitude', 'longitude']].to_dict('records')
    
    # Save the mapped data to JSON
    output_path = os.path.join(os.path.dirname(__file__), "snotel_stations_by_region.json")
    with open(output_path, "w") as f:
        json.dump(region_stations, f, indent=2)
    
    # Print summary
    print("SNOTEL Stations mapped to regions:")
    for region, stations in region_stations.items():
        print(f"{region}: {len(stations)} stations")
    
    return region_stations, region_polygons.keys()

def fetch_snotel_station_data(station_triplet, begin_date, end_date, elements_dict):
    """Fetch time series data for a specific SNOTEL station for a date range"""
    data_url = BASE_URL + "data"
    # Join all element codes for the API request
    elements = ",".join(elements_dict.values())  # e.g., "SNWD,WTEQ,SNTMP,..."
    params = {
        "stationTriplets": station_triplet,
        "beginDate": begin_date,
        "endDate": end_date,
        "elements": elements,
        "duration": "DAILY",
        "centralTendencyType": "AVERAGE"
    }
    response = requests.get(data_url, params=params)
    if response.ok:
        data = response.json()
        return data
    else:
        print(f"Request failed for {station_triplet} with status code: {response.status_code}")
        return None

def fetch_and_process_time_series_data(start_date=None, end_date=None):
    """Fetch and process time series data for all SNOTEL stations"""
    print("Fetching and processing SNOTEL time series data...")

    # Define SNOTEL elements to fetch
    snotel_elements = {
        "Snow_Depth": "SNWD",
        "Snow_Water_Equivalent": "WTEQ",
        "Snow_Temperature": "SNTMP",
        "Air_Temperature_Max": "TMAX",
        "Air_Temperature_Min": "TMIN",
        "Air_Temperature_Avg": "TAVG",
        "Precipitation_Accumulation": "PRCP",
        "Precipitation_Increment": "PREC"
    }
    
    # Get the end date
    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_obj = date.today()
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    # Get the start date
    ts_csv_path = os.path.join(os.path.dirname(__file__), "snotel_ts_data.csv")
    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    elif os.path.exists(ts_csv_path) and os.path.getsize(ts_csv_path) > 0:
        # If file exists, find the most recent date and start from there
        try:
            df = pd.read_csv(ts_csv_path)
            if not df.empty and 'Date' in df.columns:
                latest_date = pd.to_datetime(df['Date']).max()
                # Start from the day after the latest date in the file
                start_date_obj = latest_date.date() + timedelta(days=1)
                print(f"Found existing data up to {latest_date.strftime('%Y-%m-%d')}, starting from {start_date_obj.strftime('%Y-%m-%d')}")
            else:
                # Default start date if file exists but has no data
                start_date_obj = end_date_obj - timedelta(days=30)
                print("Existing file has no valid date column, starting from 30 days ago")
        except Exception as e:
            print(f"Error reading existing data file: {e}")
            start_date_obj = end_date_obj - timedelta(days=30)
            print("Using default start date (30 days ago)")
    else:
        # Default to 30 days ago if no file exists
        start_date_obj = end_date_obj - timedelta(days=30)
        print("No existing data file, starting from 30 days ago")
    
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    
    # If start date is after or equal to end date, there's nothing to do
    if start_date_obj >= end_date_obj:
        print("Start date is not before end date. No new data to fetch.")
        return True

    # Map stations to regions
    region_stations, regions = map_stations_to_regions()
    
    if not region_stations:
        print("Failed to map stations to regions.")
        return False
    
    # Prepare to collect data
    snotel_data_list = []
    
    # Fetch data for each station in each region
    for region in regions:
        if region in region_stations:
            for station in region_stations[region]:
                station_id = station["stationId"]
                station_triplet = f"{station_id}:UT:SNTL"
                print(f"Fetching data for {station['name']} in {region}...")
                
                response_data = fetch_snotel_station_data(
                    station_triplet, start_date_str, end_date_str, snotel_elements
                )
                
                if response_data:
                    for site_data in response_data:  # This is the outer list of station data
                        # Access the data array for this station
                        for element_data in site_data.get("data", []):
                            # Get the element code
                            element_code = element_data["stationElement"]["elementCode"]
                            
                            # Process each value in this element's values array
                            for value_item in element_data.get("values", []):
                                # Check if this is the first time we're seeing this date
                                date_str = value_item["date"]
                                
                                # Find or create a new record for this date
                                element_values = next((item for item in snotel_data_list 
                                                    if item["Date"] == date_str and 
                                                    item["Station_Name"] == station["name"]), None)
                                
                                if element_values is None:
                                    # Create a new record with metadata
                                    element_values = {
                                        "Date": date_str,
                                        "Station_Name": station["name"],
                                        "Region": region,
                                        "Elevation": station["elevation"],
                                        "Latitude": station["latitude"],
                                        "Longitude": station["longitude"]
                                    }
                                    snotel_data_list.append(element_values)
                                
                                # Map this element to its corresponding DataFrame column name
                                for df_name, api_code in snotel_elements.items():
                                    if api_code == element_code:
                                        element_values[df_name] = value_item.get("value")
                                        # If you want to store averages too
                                        if "average" in value_item:
                                            element_values[f"{df_name}_avg"] = value_item.get("average")
    
    # Convert list of dictionaries to DataFrame
    if snotel_data_list:
        df_new = pd.DataFrame(snotel_data_list)
        
        # Sort by Date and Station_Name
        df_new = df_new.sort_values(['Date', 'Station_Name'])
        
        # If file exists and has data, append to it, otherwise create new file
        if os.path.exists(ts_csv_path) and os.path.getsize(ts_csv_path) > 0:
            try:
                # First try to read header structure
                df_old_header = pd.read_csv(ts_csv_path, nrows=0)
                
                # Make sure the new data has all columns from the existing file
                for col in df_old_header.columns:
                    if col not in df_new.columns:
                        df_new[col] = None
                
                # Append to CSV without writing headers
                df_new.to_csv(ts_csv_path, mode='a', header=False, index=False)
                print(f"Appended {len(df_new)} new records to {ts_csv_path}")
            except Exception as e:
                print(f"Error appending to existing file: {e}")
                print("Creating new file instead")
                df_new.to_csv(ts_csv_path, index=False)
                print(f"Saved {len(df_new)} records to new file {ts_csv_path}")
        else:
            # Create new file
            df_new.to_csv(ts_csv_path, index=False)
            print(f"Created new file {ts_csv_path} with {len(df_new)} records")
        
        return True
    else:
        print("No new data was collected.")
        return True

def process_snotel_data(start_date=None, end_date=None):
    """Main function to process SNOTEL data: fetch, map to regions, and update elevations"""
    success = fetch_snotel_data(start_date, end_date)
    if success:
        # Map stations to regions
        region_stations, regions = map_stations_to_regions()
        
        # Fetch time series data
        if start_date or end_date:
            fetch_and_process_time_series_data(start_date, end_date)
        
        return True
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process SNOTEL data')
    parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--timeseries_only', action='store_true', help='Only fetch time series data without updating station metadata')
    
    args = parser.parse_args()
    
    if args.timeseries_only:
        # Only fetch and process time series data
        fetch_and_process_time_series_data(args.start_date, args.end_date)
    else:
        # Full process including metadata and time series
        process_snotel_data(args.start_date, args.end_date)