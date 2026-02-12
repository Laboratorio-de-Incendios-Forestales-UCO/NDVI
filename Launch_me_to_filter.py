# -*- coding: utf-8 -*-
print(
"""
TRACKING:
    https://doi.org/10.5281/zenodo.18620398
    Product developped by LABIF-UCO ("https://labif.es/").
    Version 20260212a (last modified by Juanan).
    
OBJECTIVE:
    To filter all the products pending to filter of the "Normalised Difference Vegetation Index 2014-present (raster 300 m), global, 10-daily – version 3" (DOI: "https://doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465") product of CLMS, excluding all pixels with:
        - An uncertainty greater than desired
        - A number of observations smaller than desired
        - Selected flags (See table 7 in the user manual)
    
INFORMATION:
    https://land.copernicus.eu/en/products/vegetation/normalised-difference-vegetation-index-v3-0-300m 
    Generated using European Union's Copernicus Land Monitoring Service information; doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465.

    This script does as follows:

    1) Looks for the NC files (by default, in the Section_DOWNLOAD\Outputs_downloaded) that have not been processed yet (i.e. that are not in Section_PROCESSING\Outputs_filtered). Take these NC files and:
    2) Filter the pixels of the main variable that have an Uncertainty greater or equal than a threshold (by default, this filter is enabled and set to 0.15)
    3) Filter the pixels of the main variable that have a Number of Observations lower than a threshold (by default, this filter is enabled and set to 2)
    4) Filter the pixels that have some flags (by default, all flags are filtered out)
    5) Save the final product in Section_PROCESSING\Outputs_filtered. This final product contains only the main variable, filtered.

EXAMPLES:
    
    run Launch_me_to_filter.py --Filter_by_uncertainty_off --Filter_by_NOBS_off --Filter_bits
        This example runs the script will all filters deactivated (i.e. the saved NC file will be equal to the original)
    
    run Launch_me_to_filter.py
    run Launch_me_to_filter.py --Thr_uncertainty 0.15 --Thr_NOBS 2 --Filter_bits 0 1 2 3 4 5 6 7
    run Launch_me_to_filter.py --Filter_by_uncertainty_on --Thr_uncertainty 0.15 --Filter_by_NOBS_on --Thr_NOBS 2 --Filter_bits 0 1 2 3 4 5 6 7
        These three examples do exactly the same, they as correspond with the default input values
    
    run Launch_me_to_filter.py --Thr_uncertainty 0.28 --Filter_by_NOBS_off --Filter_bits 0 2 4 7
        This example sets the threshold for uncertainty to 0.28 (i.e. all pixels with uncertainties equal or greater than 0.28 will be excluded), deactivates the filter for the Number of Observations and only excludes those pixels with flags in the bits 0, 2, 4 and 7.
    
WARNINGS:
    Excessively long processing times (>10 mins) could indicate an unsuitable chunk of the file for your computer.
"""
)
print("RUN THE SCRIPT:")

# %% IMPORT THE LIBRARIES
print("         Import the libraries");

from pathlib import Path as Path

import argparse as argparse
import numpy as np
import os as os
import sys as sys
import time as time
import xarray as xr

# %% PREVIOUS INFORMATION
# This Script requires the following information to run smoothly:
print("         Get previous information");

# Input 0: preconfiguration
os.chdir(Path(__file__).resolve().parent)

# Default values for the filters:

# If "Filter_uncertainty=True", then pixels with an uncertainty equal or greater than Thr_uncertainty will be filtered out.
# Thr_uncertainty must be in the range [0-1]
Filter_uncertainty = True
Thr_uncertainty = 0.15 

# If "Filter_NOBS=True", then pixels with a nomber of observations smaller than Thr_NOBS will be filtered out.
# Thr_NOBS must be in the range [0-32]      
Filter_NOBS = True
Thr_NOBS = 2 # If "Filter_NOBS=True", then pixels with a Number of OBServations lower than Thr_NOBS will be filtered out.

Filter_bitwise = {
    0: True, # bit 0 = 1: No observations found in the compositing window in, at least, one of the red or NIR bands. Set as True to exclude these pixels.
    1: True, # bit 1 = 1: At least one of the observations in the compositing window is flagged as ‘snow’. Set as True to exclude these pixels.
    2: True, # bit 2 = 1: At least one TOC-r input band for red has quality ‘warning’. Set as True to exclude these pixels.
    3: True, # bit 3 = 1: At least one TOC-r input band for red has quality ‘extreme warning’. Set as True to exclude these pixels.
    4: True, # bit 4 = 1: At least one TOC-r input band for NIR has quality ‘warning’. Set as True to exclude these pixels.
    5: True, # bit 5 = 1: At least one TOC-r input band for NIR has quality ‘extreme warning’. Set as True to exclude these pixels.
    6: True, # bit 6 = 1: TOC-r out of range (TOC-r < 0 | TOC-r > 1) for at least one band. Set as True to exclude these pixels.
    7: True, # bit 7 = 1: The BRDF MCD43P priors are gap filled. Set as True to exclude these pixels.
} # Pixels with at least a bit set as True will be filtered out. Pixels with all bits set as False will remain.



# %% ANCILLARY FUNCTIONS

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Filter NDVI products with configurable thresholds"
    )

    # --- UNCERTAINTY FILTER ---
    parser.add_argument(
        "--Filter_by_uncertainty_on",
        dest="Filter_uncertainty",
        action="store_true",
        help="Enable filtering by uncertainty"
    )

    parser.add_argument(
        "--Filter_by_uncertainty_off",
        dest="Filter_uncertainty",
        action="store_false",
        help="Disable filtering by uncertainty"
    )

    parser.set_defaults(Filter_uncertainty=Filter_uncertainty)

    parser.add_argument(
        "--Thr_uncertainty",
        type=float,
        default=Thr_uncertainty,
        help="Uncertainty threshold (0–1)"
    )

    # --- NOBS FILTER ---
    parser.add_argument(
        "--Filter_by_NOBS_on",
        dest="Filter_NOBS",
        action="store_true",
        help="Enable filtering by number of observations"
    )

    parser.add_argument(
        "--Filter_by_NOBS_off",
        dest="Filter_NOBS",
        action="store_false",
        help="Disable filtering by number of observations"
    )

    parser.set_defaults(Filter_NOBS=Filter_NOBS)

    parser.add_argument(
        "--Thr_NOBS",
        type=int,
        default=Thr_NOBS,
        help="Minimum number of observations required"
    )

    # --- QFLAG BITS ---
    parser.add_argument(
        "--Filter_bits",
        type=int,
        nargs="*",
        default=[bit for bit, val in Filter_bitwise.items() if val],
        help="Bits to exclude (e.g. --Filter_bits 0 1 3). If omitted, defaults are used."
    )

    return parser.parse_args()


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
        "Outputs_filtered": Directory_general / "Outputs_filtered",
        "Scripts": Directory_general / "Scripts",
    }  # Add new lines if nedded
    
    # Make sure that these directories exist. If not, create them.
    for directory_name, directory_path in Directories.items():
        if not directory_path.exists():
            directory_path.mkdir(parents=True)
            print(f"           - Created directory: {directory_name} → {directory_path}")

    return Directories


def f_list_of_available_NC_files(Input_NC_folder):
    print(f"         Running: {f_list_of_available_NC_files.__name__}()")
    
    try:
        # Transform from string to a windows Path
        Input_NC_folder = Path(Input_NC_folder)
           
        # List of NC files in the input folder (without the ".nc" extension)
        list_of_available_NC_files = [f.name for f in Input_NC_folder.glob("*.nc")]
        
        # Check if the folder contains any NC file
        if not list_of_available_NC_files:
            raise FileNotFoundError(f"           - WARNING: No NC files found in: {Input_NC_folder}. Check if the directory is the correct one.")
        
        # Print the number of available NC files
        Number_of_NC_files = len(list_of_available_NC_files)
        print(f"           - {Number_of_NC_files} NC files available")
        
        return list_of_available_NC_files
    
    except FileNotFoundError as e:
        print(f"{e}")
        sys.exit(1) 


def f_list_of_processed_NC_files(Directories):
    print(f"         Running: {f_list_of_processed_NC_files.__name__}()")
    
    # List of NC files in the output folder (without the ".nc" extension)
    list_of_processed_NC_files = [f.name for f in Directories["Outputs_filtered"].glob("*.nc")]
    
    # Print the number of processed NC files
    Number_of_NC_files = len(list_of_processed_NC_files)
    print(f"           - {Number_of_NC_files} NC files processed")
    
    return list_of_processed_NC_files


class AllFilesProcessed(Exception):
    """Raised when there are no NC files left to process."""
    pass


def f_Bucket_list(list_of_available_NC_files, list_of_processed_NC_files):
    print(f"         Running: {f_Bucket_list.__name__}()")
    
    try:
        
        # Compare both lists, and get a new list with the name of the files yet to download
        bucket_list = [x for x in list_of_available_NC_files if x not in list_of_processed_NC_files]
  
        # Print the number of NC files to process
        print(f"           - {len(bucket_list)} NC files to process")
        
        # Check if all the NC files have been processed
        if not bucket_list:
            raise AllFilesProcessed("           - WARNING: There are no new NC files left to process.")
        
        return bucket_list
    
    except AllFilesProcessed as e:
        print(f"{e}")
        sys.exit(1)


def f_Filter_by_uncertainty(NDVI_variable, raw_NC_ds, Filter_uncertainty, Thr_uncertainty):
    print(f"         Running: {f_Filter_by_uncertainty.__name__}()")
    
    # If the Filter by uncertainty is disabled, return the NDVI with no change
    if not Filter_uncertainty:
        print("           - WARNING: Filter by uncertainty disabled. Set it as 'True' to enable")
        return NDVI_variable
    
    # If the Filter by uncertainty is enabled, keep only the pixels where uncertainty is lower than equal or lower than Thr_uncertainty
    start = time.perf_counter()
    # Show the current Threshold
    print(f"           - Exclude pixels with uncertainty ≥ {Thr_uncertainty}")
    # To make sure the threshold is properly scaled, it is multiplied by the range in the original NC
    Thr_uncertainty = Thr_uncertainty*raw_NC_ds["NDVI_unc"].attrs.get("valid_range")[1]
    # Just pixels with uncertainties equal or lower than Thr_uncertainty will remain
    NDVI_variable = NDVI_variable.where(raw_NC_ds["NDVI_unc"] <= Thr_uncertainty, NDVI_variable.encoding.get("_FillValue"))
    
    end = time.perf_counter()
    print(f"           - The process took {end - start:.2f} seconds")
    
    return NDVI_variable


def f_Filter_by_NOBS(NDVI_variable, NOBS_variable, Filter_NOBS, Thr_NOBS):
    print(f"         Running: {f_Filter_by_NOBS.__name__}()")
    
    # If the Filter by NOBS is disabled, return the NDVI with no change
    if not Filter_NOBS:
        print("           - WARNING: Filter by NOBS disabled. Set it as 'True' to enable")
        return NDVI_variable
    
    # If the Filter by NOBS is enabled, transform the pixels with values smaller than Thr_NOBS into the value that NDVI reserves for missing pixels. Keep the rest of pixels untouched.
    start = time.perf_counter()
    # Show the current Threshold
    print(f"           - Exclude pixels with NOBS < {Thr_NOBS}")
    NDVI_variable = NDVI_variable.where(NOBS_variable >= Thr_NOBS, NDVI_variable.encoding.get("_FillValue"))
    end = time.perf_counter()
    print(f"           - The process took {end - start:.2f} seconds")
    return NDVI_variable


def f_Filter_by_QFLAGS(NDVI_variable, QFLAG_variable, Filter_bitwise):
    """
    Parameters
    ----------
    NDVI_variable: DataArray
        This is the NDVI    
    QFLAG_variable: DataArray
        This is the variable within the NC file that contains QFLAG, filled with bitwise pixels.
        Refer to the product user manual (Table 7) for further information.
    Filter_bitwise : dict
        This is a dict with True/False values. Bits set as True will be filtered out..

    Returns
    -------
    NDVI_variable: DataArray
        If Filter_bitwise contains any 'True', NDVI_variable will exclude the pixels were Mask_QFLAG is 'True'.
        If Filter_bitwise does not contain any 'True', NDVI_variable will be identical than the input.
    """
    print(f"         Running: {f_Filter_by_QFLAGS.__name__}()")
    
    # If all bits are False, return the NDVI with no change
    if all(not v for v in Filter_bitwise.values()):
        print("           - WARNING: Filter by QFLAGS disabled. Set bits as 'True' to enable")
        return NDVI_variable
    
    # If, at least, one of the bits is True. Create a mask to exclude these pixels.
    start = time.perf_counter()
    # Show the enabled filters
    print("           - Exclude pixels with the next flags:")
    for k, v in Filter_bitwise.items():
        print(f"               bit {k} set as {v}")

    # Create the mask
    reject_mask = sum(1 << bit for bit, reject in Filter_bitwise.items() if reject)
    Q = QFLAG_variable.fillna(0).astype(np.uint8, copy=False)
    Mask_QFLAG = (Q & reject_mask) == 0  
    
    # Then, transform the pixels where the mask is True into the value that NDVI reserves for missing pixels. Keep the rest of pixels untouched.
    # NDVI_variable = NDVI_variable.where(~Mask_QFLAG, NDVI_variable.encoding.get("_FillValue"))
    NDVI_variable = NDVI_variable.where(Mask_QFLAG, NDVI_variable.encoding.get("_FillValue"))

    end = time.perf_counter()
    print(f"           - The process took {end - start:.2f} seconds")
    
    return NDVI_variable
    

def f_Save_the_NC(NDVI_variable, raw_NC_ds, filename, Directories):
    print(f"         Running: {f_Save_the_NC.__name__}()")
    print("           - This process may take a few minutes")    
    start = time.perf_counter() 
    
    route_to_output_NC = Path(Directories["Outputs_filtered"]) / Path(filename)
    
    encoding = {
        "NDVI": {
            "zlib": True,
            "complevel": 4,
            "dtype": "float32",
            # "_FillValue": -9999.0,
            "chunksizes": raw_NC_ds["NDVI"].encoding.get("chunksizes")
        }
    }
    
    NDVI_ds = NDVI_variable.to_dataset(name="NDVI")
    NDVI_ds["NDVI"].attrs = raw_NC_ds["NDVI"].attrs
    
    NDVI_ds.to_netcdf(
        route_to_output_NC,
        format="NETCDF4",
        engine="netcdf4",
        encoding=encoding
    )
    
    end = time.perf_counter()
    print(f"           - The process took {end - start:.2f} seconds")
    
    
# %% MAIN FUNCTION
def main():
    print(f"         Running: {main.__name__}()")
    
    # %% LOAD THE INPUTS
    global Filter_uncertainty, Thr_uncertainty
    global Filter_NOBS, Thr_NOBS, Filter_bitwise
    
    args = parse_arguments()
    
    Filter_uncertainty = args.Filter_uncertainty
    Thr_uncertainty = args.Thr_uncertainty
    Filter_NOBS = args.Filter_NOBS
    Thr_NOBS = args.Thr_NOBS
    
    # Reconstruir diccionario de bits
    Filter_bitwise = {bit: (bit in args.Filter_bits) for bit in range(8)}
    
    # %% DEFINE THE DIRECTORIES
    Directories = f_Define_the_directories()
    
    # %% LIST ALL NC FILES THAT REMAIN UNPROCESSED
    # List all available NC files
    list_of_available_NC_files = f_list_of_available_NC_files(Directories["Outputs_downloaded"])
    # List all processed NC files.
    list_of_processed_NC_files = f_list_of_processed_NC_files(Directories)
    # Compare both lists to get the NC files that remain unprocessed
    bucket_list = f_Bucket_list(list_of_available_NC_files, list_of_processed_NC_files)
    
    # %% START THE LOOP
    # To process every NC file that remains unprocessed
    
    counter = 0
    for every_NC_file in bucket_list:
        counter = counter+1
        print()
        print(f"       **Processing NC {counter} of {len(bucket_list)} ({every_NC_file})")
        
        try:
        
            # %% OPEN THE NC FILE
            print(f"         Opening the file from {main.__name__}()")
            # Create the route to the NC file
            route_to_input_NC = Directories["Outputs_downloaded"] / Path(every_NC_file)
            # Open the input NC file (with chunks)
            # It is key to not to decode. Decoding opens it directly in PV, so 
            # values in DV (like the imtrinsic flags) are not properly detected
            raw_NC_ds = xr.open_dataset(route_to_input_NC, decode_cf=False).chunk("auto")
                        
            
            # %% CREATE THE NEW FILE
            # Create the new file
            NDVI = raw_NC_ds["NDVI"]
            # Exclude the pixels with intrinsic flags
            # 'flag_values': array([252, 253, 254, 255], dtype=uint8),
            # 'flag_meanings': 'Unknown Snow Water Missing'}
            invalid = NDVI.isin(NDVI.attrs.get("flag_values"))
            NDVI = NDVI.where(~invalid)
            
            # %% FILTER 
            # Filter by uncertainty
            NDVI = f_Filter_by_uncertainty(NDVI, raw_NC_ds, Filter_uncertainty, Thr_uncertainty)
            
            # Filter by number of observations
            NDVI = f_Filter_by_NOBS(NDVI, raw_NC_ds["NOBS"], Filter_NOBS, Thr_NOBS)
            
            # Filter by Quality Flags
            # bits set as True in Filter_bitwise will be filtered out
            NDVI = f_Filter_by_QFLAGS(NDVI, raw_NC_ds["QFLAG"], Filter_bitwise)
                      
            # %% SAVE THE PROCESSED NC FILE
            f_Save_the_NC(NDVI, raw_NC_ds, every_NC_file, Directories)
            
            # NOTES:
                # raw_NC_ds["NDVI"].encoding muestra cómo se ha abierto la variebl (por ejeplo, si se le ha aplicado el paso a PV)
            
        finally:
            # Close the open NC files
            raw_NC_ds.close()
            NDVI.close()
    
    # %% ENDSCRIPT
    print()
    print("         Endscript");
       

# %% RING BELL
if __name__ == "__main__":
    main()

