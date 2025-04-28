import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
import matplotlib.colors as mcolors
import os

def load_region_boundaries():
    """Load region boundaries from JSON file."""
    try:
        with open("region_boundaries.json", "r") as f:
            data = json.load(f)
            return data["regions"]
    except Exception as e:
        print(f"Error loading region boundaries: {e}")
        return {}

def visualize_regions():
    # Load SNOTEL data
    df = pd.read_csv("snotel_data.csv")
    
    # Load region mappings if available
    region_stations = {}
    if os.path.exists("snotel_stations_by_region.json"):
        with open("snotel_stations_by_region.json", "r") as f:
            region_stations = json.load(f)
    
    # Load region boundaries from JSON file
    region_polygons = load_region_boundaries()
    
    # Create a colormap for the regions
    colors = list(mcolors.TABLEAU_COLORS)
    
    # Prepare figure and axes
    plt.figure(figsize=(12, 10))
    ax = plt.subplot(111)
    
    # Define Utah state boundary coordinates (approximate, [lat, lon])
    utah_boundary = [
        [42.001, -114.053], [42.001, -111.046], [41.000, -111.046], 
        [41.000, -109.050], [37.000, -109.050], [37.000, -114.050], 
        [42.001, -114.053]  # Close the polygon
    ]
    
    # Plot Utah state outline
    utah_coords = np.array(utah_boundary)[:, [1, 0]]  # Swap to [lon, lat]
    utah_polygon = Polygon(utah_coords, closed=True, fill=False, edgecolor='black', linewidth=2)
    ax.add_patch(utah_polygon)
    
    # Plot region polygons
    patches = []
    region_names = []
    region_centroids = {}
    
    for i, (region_name, coords) in enumerate(region_polygons.items()):
        # Convert to numpy array and swap coordinates to [x, y] (lon, lat) for plotting
        coords_array = np.array(coords)[:, [1, 0]]  # Swap lat, lon to lon, lat
        
        # Calculate centroid for label placement
        centroid_x = np.mean([p[1] for p in coords])  # longitude is x
        centroid_y = np.mean([p[0] for p in coords])  # latitude is y
        region_centroids[region_name] = (centroid_x, centroid_y)
        
        # Create polygon patch
        polygon = Polygon(coords_array, closed=True, fill=True)
        patches.append(polygon)
        region_names.append(region_name)
    
    # Create patch collection with specified colormap
    p = PatchCollection(patches, alpha=0.4)
    p.set_array(np.arange(len(patches)))
    ax.add_collection(p)
    plt.colorbar(p)
    
    # Add region labels at centroids
    for region_name, (x, y) in region_centroids.items():
        ax.text(x, y, region_name, fontsize=10, ha='center', va='center', weight='bold')
    
    # Plot SNOTEL stations
    ax.scatter(df['longitude'], df['latitude'], c='black', s=20, alpha=0.7, label='SNOTEL Stations')
    
    # Add station count labels
    for region_name, (x, y) in region_centroids.items():
        if region_name in region_stations:
            station_count = len(region_stations[region_name])
            ax.text(x, y - 0.1, f"({station_count} stations)", fontsize=8, ha='center', va='center')
    
    # Set plot limits
    ax.set_xlim(-114.5, -109.0)
    ax.set_ylim(37.0, 42.0)
    
    # Add labels and title
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Utah Avalanche Forecast Regions and SNOTEL Stations')
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Save the plot
    plt.tight_layout()
    plt.savefig('utah_regions_map.png', dpi=300)
    print(f"Map saved as utah_regions_map.png")
    plt.show()

if __name__ == "__main__":
    visualize_regions()