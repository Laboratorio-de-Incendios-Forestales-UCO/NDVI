# -*- coding: utf-8 -*-
print(
"""
TRACKING:
    Product developped by LABIF-UCO ("https://labif.es/").
    Version 20260206a (last modified by Juanan).
    The process includes scripts provided by Copernicus ("https://documentation.dataspace.copernicus.eu/APIs/S3.html").
    
OBJECTIVE:
    To download all the products pending to download of the "Normalised Difference Vegetation Index 2014-present (raster 300 m), global, 10-daily – version 3" (DOI: "https://doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465") product of CLMS.
    
INFORMATION:
    https://land.copernicus.eu/en/products/vegetation/normalised-difference-vegetation-index-v3-0-300m 
    Generated using European Union's Copernicus Land Monitoring Service information; doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465.
    
WARNINGS:
    The directory must contain a ".credentials.ini" file with your credentials to log in into CDSE (https://dataspace.copernicus.eu/).
    This file must follow the next structure:
        
      [cdse]
      username = username@example.org
      password = MyPa5sWoRd

    CDSE sets quotas and limitations for the downloads ("https://documentation.dataspace.copernicus.eu/Quotas.html"). Reaching these quotas and limitations triggers a Runtime Error while creating temporary S3 credentials (error #403). 
"""
)
print("RUN THE SCRIPT:")

# %% IMPORT THE LIBRARIES
print("         Import the libraries.");

from pathlib import Path as Path
from tqdm import tqdm as tqdm

import argparse as argparse
import boto3 as boto3
import configparser as configparser
import json as json
import os as os
import pandas as pd
import requests as requests
import sys as sys
import time as time


# %% PREVIOUS INFORMATION
# This Script requires the following information to run smoothly:
print("         Get previous information.");

# Input 0: preconfiguration
os.chdir(Path(__file__).resolve().parent)

# Input 1: configuration
config = {
    "auth_server_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
    "odata_base_url": "https://catalogue.dataspace.copernicus.eu/odata/v1/Products",
    "s3_endpoint_url": "https://eodata.dataspace.copernicus.eu",
}


# %% ANCILLARY FUNCTIONS

def f_Define_the_directories():
    """
    Returns
    -------
    Directories : dict
        Dictionary with the routes to the defined directories.
    """
    print(f"         Running: {f_Define_the_directories.__name__}()")
    
    Directory_general = Path(__file__).resolve().parent.parent

    Directories = {
        "General": Directory_general,
        "Ancillary": Directory_general / "Ancillary",
        "Inputs": Directory_general / "Inputs",
        "Outputs_downloaded": Directory_general / "Outputs_downloaded",
        "Scripts": Directory_general / "Scripts",
    }  # Add new lines if nedded
    
    # Make sure that these directories exist. If not, create them.
    for directory_name, directory_path in Directories.items():
        if not directory_path.exists():
            directory_path.mkdir(parents=True)
            print(f"           - Created directory: {directory_name} → {directory_path}")

    return Directories


def f_Import_credentials(filename=".credentials.ini"):
    """
    Reads CDSE credentials from an ini file located in the same
    directory as this script.
    """
    print(f"         Running: {f_Import_credentials.__name__}()")
    
    
    config = configparser.ConfigParser()

    cred_path = Path(__file__).parent / filename

    if not cred_path.exists():
        raise FileNotFoundError(
            f"           - Credentials file not found: {cred_path}"
        )

    config.read(cred_path)

    try:
        _username = config["cdse"]["username"]
        _password = config["cdse"]["password"]
        
    except KeyError as e:
        raise KeyError(
            "           - Missing [cdse] section or keys in credentials file"
        ) from e

    return _username, _password


def f_download_csv_file(route_to_download_the_csv, filename, target_url):
    
    """
    Parameters
    ----------
    route_to_download_the_csv : WindowsPath
        Route to save the downloaded CSV.

    Returns
    -------
    None.
    """
    print(f"         Running: {f_download_csv_file.__name__}()")
    
    route_to_csv = route_to_download_the_csv / filename

    # Download the file
    response = requests.get(target_url, timeout=60)
    response.raise_for_status()

    # Save it
    route_to_csv.write_bytes(response.content)


def f_list_of_available_files(route_where_the_csv_were_downloaded, filename, desired_column):
    
    """
    Parameters
    ----------
    route_where_the_csv_were_downloaded: WindowsPath
        Route to the CSV to read.

    Returns
    -------
    list_of_available_files: list
    List within the csv with the available files to download.
    """
    print(f"         Running: {f_list_of_available_files.__name__}()")
    
    route_to_csv = route_where_the_csv_were_downloaded / filename
    
    df = pd.read_csv(route_to_csv, sep=";")
    
    # Extract the column that contains the desired information
    list_of_available_files = df.iloc[:, desired_column].tolist()
    
    return list_of_available_files


def  f_list_of_current_files(route_to_folder):
    
    """
    Parameters
    ----------
    route_to_folder : WindowsPath
        Route to the folder where the script must look for the already downloaded NC files.

    Returns
    -------
    list_of_current_files : list
        NC files already in the folder.
    """
    print(f"         Running: {f_list_of_current_files.__name__}()")
    
    # List of NC files in the folder
    list_of_current_files = [f.stem for f in route_to_folder.glob("*.nc")]
    
    # List of NC files in the folder, adding _NC to be comparable with the names in the CSV
    list_of_current_files = [s + "_nc" for s in list_of_current_files]

       
    return list_of_current_files


def f_Bucket_list(Directories):
    
    """
    Parameters
    ----------
    Directories : dict
        Dictionary with, at least, two WindowsPath: one named Inputs were the CSV will be saved, and another one named Outputs, where the already downloaded files should be

    Returns
    -------
    bucket_list : list
        List with the names of the NC files to download.

    """
    print(f"         Running: {f_Bucket_list.__name__}()")
    
    # Target URL for the request
    target_url = "https://s3.waw3-1.cloudferro.com/swift/v1/CatalogueCSV/bio-geophysical/vegetation_indices/ndvi_global_300m_10daily_v3/ndvi_global_300m_10daily_v3_nc.csv"

    # Define the filename of the requested file
    filename = "ndvi_global_300m_10daily_v3_nc.csv"
    
    # Column of the csv file that contains the desired information
    # Take into account that the first column gets the number 0
    desired_column = 1
    
    try:
              
        # Download the csv file containing the available files
        # Note that it will be downloaded in the input folder, as it will be an 
        # input during the rest of the script
        f_download_csv_file(Directories["Inputs"], filename, target_url)
        
        # Read the csv and get a list with the names of the available files to download
        list_of_available_files = f_list_of_available_files(Directories["Inputs"], filename, desired_column)
        
        # Check which files were already downloaded, and are thus in the Outputs folder
        list_of_current_files = f_list_of_current_files(Directories["Outputs_downloaded"])
        
        # Compare both lists, and get a new list with the name of the files yet to download
        bucket_list = [x for x in list_of_available_files if x not in list_of_current_files]
  
        print(f"           - Of the {len(list_of_available_files)} available files, {len(bucket_list)} remain to be downloaded")
        
        return bucket_list
    
    except Exception as e:
        print(f"Error: {e}")
        # Abort in case of critical error
        sys.exit(1)


def get_access_token(config, _username, _password):
    print(f"         Running: {get_access_token.__name__}()")
    auth_data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": _username,
        "password": _password,
    }
    response = requests.post(
        config["auth_server_url"],
        data=auth_data,
        verify=True,
        allow_redirects=False,
    )

    if response.status_code == 200:
        return response.json()["access_token"]
    raise RuntimeError(
        f"Failed to retrieve access token ({response.status_code})"
    )


def get_eo_product_details(config, headers, eo_product_name):
    print(f"         Running: {get_eo_product_details.__name__}()")
    odata_url = (
        f"{config['odata_base_url']}?$filter=Name eq '{eo_product_name}'"
    )
    response = requests.get(odata_url, headers=headers)

    if response.status_code == 200:
        product = response.json()["value"][0]
        return product["Id"], product["S3Path"]

    raise RuntimeError(
        f"Failed to retrieve EO product details ({response.status_code})"
    )


def get_temporary_s3_credentials(headers):
    print(f"         Running: {get_temporary_s3_credentials.__name__}()")
    
    response = requests.post(
        "https://s3-keys-manager.cloudferro.com/api/user/credentials",
        headers=headers,
    )

    if response.status_code == 200:
        return response.json()

    raise RuntimeError(
        f"Failed to create temporary S3 credentials ({response.status_code})"
    )


def format_filename(filename, length=40):
    print(f"         Running: {format_filename.__name__}()")
    return (
        filename[: length - 3] + "..."
        if len(filename) > length
        else filename.ljust(length)
    )


def download_file_s3(s3, bucket, key, local_path, failed):
    print(f"         Running: {download_file_s3.__name__}()")
    try:
        size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        name = format_filename(os.path.basename(local_path))

        with tqdm(
            total=size,
            unit="B",
            unit_scale=True,
            desc=name,
            ncols=80,
        ) as bar:

            def cb(bytes_amount):
                bar.update(bytes_amount)

            s3.download_file(bucket, key, local_path, Callback=cb)

    except Exception as e:
        failed.append(key)
        print(f"Download failed: {key} ({e})")


def traverse_and_download_s3(s3_resource, bucket, prefix, local_root, failed):
    print(f"         Running: {traverse_and_download_s3.__name__}()")
    bucket_obj = s3_resource.Bucket(bucket)

    for obj in bucket_obj.objects.filter(Prefix=prefix):
        rel = os.path.relpath(obj.key, prefix)
        dest = os.path.join(local_root, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        download_file_s3(
            s3_resource.meta.client,
            bucket,
            obj.key,
            dest,
            failed,
        )



def f_Downloader(_username, _password, eo_product_name, config, output_dir):
    print(f"         Running: {f_Downloader.__name__}()")
    access_token = get_access_token(config, _username, _password)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    _, s3_path = get_eo_product_details(
        config, headers, eo_product_name
    )
    bucket, prefix = s3_path.lstrip("/").split("/", 1)

    s3_creds = get_temporary_s3_credentials(headers)

    time.sleep(5)

    s3_resource = boto3.resource(
        "s3",
        endpoint_url=config["s3_endpoint_url"],
        aws_access_key_id=s3_creds["access_id"],
        aws_secret_access_key=s3_creds["secret"],
    )

    # os.makedirs(eo_product_name, exist_ok=True)

    failed = []
    traverse_and_download_s3(
        s3_resource,
        bucket,
        prefix,
        # os.path.join(output_dir, eo_product_name),
        output_dir,
        failed,
    )

    requests.delete(
        f"https://s3-keys-manager.cloudferro.com/api/user/credentials/access_id/"
        f"{s3_creds['access_id']}",
        headers=headers,
    )

    if failed:
        raise RuntimeError(
            f"Download incomplete ({len(failed)} files failed)"
        )
        

# %% MAIN FUNCTION
def main():
    print(f"         Running: {main.__name__}()")
    
    # %% DEFINE THE DIRECTORIES
    Directories = f_Define_the_directories()
    
    # %% IMPORT THE CREDENTIALS
    # Credentials to log in CDSE (https://dataspace.copernicus.eu/)
    _username, _password = f_Import_credentials()
    
    # %% QUEUE THE BUCKET LIST
    # Download the csv with all the available dates.
    # Note that this will be downloaded in the input folder, as it will be an input in a later process
    # Then, compare the available dates with the currently downloaded ones and
    # Create a bucket list with all the files yet to download
    Bucket_list = f_Bucket_list(Directories)
    
    # %% DOWNLOAD THE PRODUCTS
    counter = 0;
    for eo_product_name in Bucket_list:
        
        print()
        print(f"       **Downloading {eo_product_name}")
        
        f_Downloader(_username, _password, eo_product_name, config, Directories["Outputs_downloaded"])

        counter = counter+1;    
        print(f"           - {counter} files downloaded. {len(Bucket_list)-counter} files remaining.")

    # %% ENDSCRIPT
    print()
    print("         Endscript");
    
    
# %% RING BELL
if __name__ == "__main__":
    main()
