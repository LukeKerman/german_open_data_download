# GeoPortal Tile Downloader

## Project Overview

This project is designed to facilitate the download and processing of various geospatial data tiles from multiple sources across Germany. The scripts handle different data types, including Digital Terrain Models (DTM), Digital Surface Models (DSM), and Digital Orthophotos (DOP). The data is fetched, processed, and optionally uploaded to an S3 bucket.

## Directory Structure

The project contains several key components:
- `main.py`: The main script orchestrating the download and processing of the tiles.
- `state_tile_creator.py`: A script that generates the tiles for specific states.
- `config.json`: A configuration file specifying the download links, metadata links, and S3 paths for different states and data types.
- `init.json`: A JSON file containing initialization parameters such as area of interest (AOI) path, data type, and other settings.
- `download_scripts/{state}_download.py`: Handle the download procedure of each data source.

## Configuration Files

### `init.json`

This file contains initialization parameters used by the main script. Key parameters include:

- `aoi_path`: Path to the Area of Interest (AOI) file in GeoJSON format or CSV file for a single state.
- `data_type`: Type of data to download (e.g., DTM, DSM, DOP).
- `selected_states`: States selected for processing (null implies all states). If CSV file is used as AOI, corresponding state must be specified.
- `meta_path`: Path to the metadata file.
- `local_landing_path`: Local directory for storing downloaded files.
- `date_range`: Optional date range for filtering data. (Date format: Y%-m%-d%)
- `upload_s3`: Boolean indicating whether to upload processed data to S3.
- `delete`: Boolean indicating whether to delete local files after processing.

### Example `init.json`:

```json
{
  "aoi_path": "/path/to/aoi.geojson",
  "data_type": "DTM",
  "selected_states": null,
  "meta_path": "meta/tile_data.json",
  "local_landing_path": "tmp",
  "date_range": {
    "begin": null,
    "end": null
  },
  "upload_s3": false,
  "delete": false
}
```

### config.json
This file defines configurations for different data types and states, including tile sizes, download links, metadata links, and S3 paths.

### Example config.json:
```json
{
  "DOP": {
    "Default": {
      "tile_info": {
        "tile_size": 1000,
        "x": 0,
        "y": 0
      },
      "links": {
        "download_link": "http://example.com/Default/download",
        "meta_data_link": "http://example.com/Default/meta",
        "s3_path": "s3://default/dop/"
      }
    },
    "BB": {
      "tile_info": {
        "tile_size": 1000,
        "x": 0,
        "y": 0
      },
      "links": {
        "download_link": "https://data.geobasis-bb.de/geobasis/daten/dop/rgbi_tif/dop_{}.zip",
        "meta_data_link": "https://data.geobasis-bb.de/geobasis/information/aktualitaeten/bb_dop_aktualitaet.csv",
        "s3_path": "s3://bb/dop/"
      }
    }
    // Additional configurations for other states and data types...
  }
}
```

# Scripts

## main.py
The main script responsible for orchestrating the download and processing of geospatial tiles. It reads initialization parameters from `init.json` and configurations from `config.json`.

## state_tile_creator.py
This script generates the necessary tiles for specific states based on the configurations provided in `config.json`.

# Download Scripts

The download scripts are named following the convention `{state_abbreviation}_download.py` and are responsible for the individual download of data files and metadata files specific to each state. These scripts ensure that the appropriate datasets are fetched and organized correctly, based on the configurations provided.

For example:
- `mv_download.py`: A script for downloading data specific to Mecklenburg-Vorpommern (MV).
- `bw_download.py`: A script for downloading data specific to Baden-Württemberg (BW).

Each script handles the unique requirements and endpoints associated with the state's data, making it easier to manage and update the datasets as needed. These scripts leverage the configurations defined in `config.json` to ensure consistency and accuracy in the downloaded data.

# Usage

1. Ensure `init.json` and `config.json` are properly configured.
2. Run `main.py` to start the download and processing pipeline.

```bash
python main.py
```

**Note:** This project is currently under construction. The README will be further updated in the future with more detailed information.