import json
import os
import requests
from zipfile import ZipFile, is_zipfile
from cloudpathlib import CloudPath
from datetime import datetime

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

class DownloadTools:
    def load_json(self, file_path):
        """
        Load a JSON file from the specified file path.

        Parameters:
        - file_path: Path to the JSON file.

        Returns:
        - The loaded JSON data.
        """
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def save_json(self, file_path, file):
        """
        Save data to a JSON file at the specified file path.

        Parameters:
        - file_path: Path to the JSON file.
        - file: Data to be saved.
        """
        with open(file_path, 'w') as f:
            json.dump(file, f, indent=4)

    def upload_file(self, file_path, target):
        s3_target = CloudPath(target)
        s3_target.upload_from(file_path)

    def delete_files_and_dir(self, dir):
        files_and_dir = os.listdir(dir)
        for item in files_and_dir:
            item_path = os.path.join(dir, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                self.delete_files_and_dir(item_path)
        os.rmdir(dir)

    def download_file(self, download_url, save_path, tile_info):

        def print_progress(total, chunksize, tile_info):
            if total:
                progress = (chunksize / total) * 100
                if progress == 100:
                    print(f"\rDownload progress of tile {tile_info['tile_name']}:\t{progress:>.1f}% completed", end="")
                else:
                    print(f"\rDownload progress of tile {tile_info['tile_name']}:\t{progress:>.1f}% ({total/(1024 * 1024):.1f} MB)", end="")
            else:
                print(f"\rDownload of tile {tile_info['tile_name']}: {chunksize / (1024 * 1024):.1f} MB", end="")

        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        response = requests.get(download_url, stream=True, verify=False)
        total_size = int(response.headers.get('content-length', 0))
        content_type = response.headers.get('Content-Type', 0)
        chunk_size = 512*1024 # 0.5 MByte
        downloaded_size = 0
        
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        print_progress(total_size, downloaded_size, tile_info)
        else:
            raise Exception(f"Failed to retrieve content. Status code: {response.status_code}")
        
        if not total_size:
            print(f"\rDownload of tile {tile_info['tile_name']} completed ({downloaded_size / (1024 * 1024):.1f} MB)", end="")

        if is_zipfile(save_path):
            with ZipFile(save_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                extract_path = os.path.dirname(save_path)
                if len(file_list) == 1:
                    zip_ref.extractall(extract_path)
                else:
                    for member in zip_ref.namelist():
                        filename = os.path.basename(member)
                        # Skip directories and empty filenames
                        if not filename:
                            continue
                        # Define the target path for the extracted file
                        target_path = os.path.join(extract_path, filename)
                        # Open the source file from the zip archive
                        source = zip_ref.open(member)
                        # Ensure the target directory exists
                        os.makedirs(os.path.dirname(target_path), exist_ok=True)
                        # Write the file to the target directory
                        with open(target_path, "wb") as target:
                            target.write(source.read())
                    '''extract_dir = os.path.join(os.path.dirname(save_path), os.path.splitext(os.path.basename(save_path))[0])
                    os.makedirs(extract_dir, exist_ok=True)
                    zip_ref.extractall(extract_dir)'''
            os.remove(save_path)

    def find_file(self, save_path):
        # Create the directory path by removing the .zip extension
        if "zip" in save_path:
            dir_path = save_path[:-4]
        else:
            dir_path = save_path

        # List of file extensions to look for
        extensions = ('.tif', '.xyz', '.laz', '.las')
        found_files = []

        # Search for files with the specified extensions
        for root, _, files in os.walk(dir_path):
            for file in files:
                if file.endswith(extensions):
                    file_path = os.path.join(root, file)
                    found_files.append(file_path)
        
        if len(found_files) == 1:
            return found_files[0]
        elif len(found_files) > 1:
            return found_files
        else:
            print(f"No relevant files found in {dir_path}")
        return None
    
    def within_date_range(self, tile_timestamp, date_range):

        def parse_date(date_str):
            date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"]
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Date format of '{date_str}' is not supported")

        # Convert tile_timestamp to datetime object
        if tile_timestamp:
            tile_date = parse_date(tile_timestamp)
        else:
            return True
        
        # Extract and convert begin and end dates from date_range
        begin_date = date_range.get("begin")
        end_date = date_range.get("end")
        
        if begin_date is not None:
            begin_date = datetime.strptime(begin_date, "%Y-%m-%d")
        if end_date is not None:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Perform the date range check
        if begin_date is not None and end_date is not None:
            return begin_date <= tile_date <= end_date
        elif begin_date is not None:
            return tile_date >= begin_date
        elif end_date is not None:
            return tile_date <= end_date
        else:
            return True
        
    def filter_tiles_by_date(self, tiles, date_range):

        def parse_date(date_str):
            date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"]
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            raise ValueError(f"Date format of '{date_str}' is not supported")
        
        def within_date_range(tile_timestamp, date_range):
            # Return true if no timestamp is provided
            if not tile_timestamp:
                return True
            
            # Convert tile_timestamp to datetime object
            tile_date = parse_date(tile_timestamp)

            # Extract and convert begin and end dates from date_range
            begin_date = date_range.get("begin")
            end_date = date_range.get("end")
            
            if begin_date is not None:
                begin_date = datetime.strptime(begin_date, "%Y-%m-%d")
            if end_date is not None:
                if end_date.startswith("XXXX"):
                    end_date = begin_date.replace(month=10, day=30)
                    extend_years = True
                else:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")
                    extend_years = False
            else:
                extend_years = False

            # If both dates are None, return True (no date filtering)
            if begin_date is None and end_date is None:
                return True

            # Generate all vegetation periods from begin_date up to tile_date's year
            periods = []
            if extend_years:
                current_year = datetime.now().year
                for year in range(begin_date.year, current_year + 1):
                    period_start = begin_date.replace(year=year)
                    period_end = end_date.replace(year=year)
                    periods.append((period_start, period_end))
            else:
                periods.append((begin_date, end_date))

            # Perform the date range check across all periods
            for start, end in periods:
                if start is not None and end is not None and start <= tile_date <= end:
                    return True
                elif start is not None and end is None and tile_date >= start:
                    return True
                elif start is None and end is not None and tile_date <= end:
                    return True

            return False

        return [tile for tile in tiles if within_date_range(tile["timestamp"], date_range)]