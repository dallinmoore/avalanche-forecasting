import os
import pandas as pd
import subprocess
import datetime
import sys

# Define file paths
SNOTEL_DATA_PATH = os.path.join('snotel', 'snotel_data.csv')
AVALANCHE_DATA_PATH = 'avalanche-forecast-rose.csv'
SNOTEL_PROCESSOR_PATH = os.path.join('snotel', 'snotel_processor.py')
AVALANCHE_PROCESSOR_PATH = os.path.join('update_avalanche_forecast_dataset', 'update_avalanche_forecast_dataset.py')

def get_current_date():
    """Returns the current date in YYYY-MM-DD format"""
    return datetime.datetime.now().strftime('%Y-%m-%d')

def run_processor(processor_path, start_date=None, end_date=None):
    """Run a processor script with optional date parameters"""
    command = [sys.executable, processor_path]
    
    if start_date:
        command.extend(['--start_date', start_date])
    if end_date:
        command.extend(['--end_date', end_date])
    
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, check=True)
    return result.returncode == 0

def process_snotel_data():
    """Process SNOTEL data based on file existence and latest date"""
    print("Processing SNOTEL data...")
    
    if os.path.exists(SNOTEL_DATA_PATH):
        print(f"Found existing SNOTEL data at {SNOTEL_DATA_PATH}")
        try:
            df = pd.read_csv(SNOTEL_DATA_PATH)
            if 'date' in df.columns:
                latest_date = pd.to_datetime(df['date']).max().strftime('%Y-%m-%d')
                print(f"Latest SNOTEL data date: {latest_date}")
                current_date = get_current_date()
                
                # Run processor with date range from latest date to current date
                print(f"Updating SNOTEL data from {latest_date} to {current_date}")
                return run_processor(SNOTEL_PROCESSOR_PATH, latest_date, current_date)
            else:
                print("No date column found in SNOTEL data. Rebuilding...")
                return run_processor(SNOTEL_PROCESSOR_PATH)
                
        except Exception as e:
            print(f"Error reading SNOTEL data: {e}")
            print("Rebuilding SNOTEL data...")
            return run_processor(SNOTEL_PROCESSOR_PATH)
    else:
        print("No existing SNOTEL data found. Building new dataset...")
        return run_processor(SNOTEL_PROCESSOR_PATH)

def process_avalanche_forecast_data():
    """Process avalanche forecast data based on file existence and latest date"""
    print("Processing avalanche forecast data...")
    
    if os.path.exists(AVALANCHE_DATA_PATH):
        print(f"Found existing avalanche forecast data at {AVALANCHE_DATA_PATH}")
        try:
            df = pd.read_csv(AVALANCHE_DATA_PATH)
            if 'Date Issued' in df.columns:
                latest_date = pd.to_datetime(df['Date Issued']).max().strftime('%Y-%m-%d')
                print(f"Latest avalanche forecast date: {latest_date}")
                current_date = get_current_date()
                
                # Run processor with date range from latest date to current date
                print(f"Updating avalanche forecast data from {latest_date} to {current_date}")
                return run_processor(AVALANCHE_PROCESSOR_PATH, latest_date, current_date)
            else:
                print("No date column found in avalanche forecast data. Rebuilding...")
                return run_processor(AVALANCHE_PROCESSOR_PATH)
                
        except Exception as e:
            print(f"Error reading avalanche forecast data: {e}")
            print("Rebuilding avalanche forecast data...")
            return run_processor(AVALANCHE_PROCESSOR_PATH)
    else:
        print("No existing avalanche forecast data found. Building new dataset...")
        return run_processor(AVALANCHE_PROCESSOR_PATH)

def main():
    """Main function to run data processing"""
    print("Starting data processing...")
    
    # Process SNOTEL data
    snotel_success = process_snotel_data()
    if not snotel_success:
        print("SNOTEL data processing failed")
    else:
        print("SNOTEL data processing completed successfully")
    
    # Process avalanche forecast data
    avalanche_success = process_avalanche_forecast_data()
    if not avalanche_success:
        print("Avalanche forecast data processing failed")
    else:
        print("Avalanche forecast data processing completed successfully")
    
    if snotel_success and avalanche_success:
        print("All data processing completed successfully")
        return 0
    else:
        print("Some data processing tasks failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())