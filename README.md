# NDVI
This family of scripts download, filter and process the product "Normalised Difference Vegetation Index 2014-present (raster 300 m), global, 10-daily – version 3" from Copernicus with DOI: "https://doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465".

# LICENSE
This project is licensed under the MIT License - see the LICENSE file for details.
If you use this project, please provide proper attribution to LABIF (https://labif.es/).

# DESCRIPTION OF THE PRODUCTS

# Launch_me_to_download_NDVI
Version 20260206a (Last modified by @JuananMunoz)
Run Launch_me_to_download_NDVI.py to download all the products pending to download of the "Normalised Difference Vegetation Index 2014-present (raster 300 m), global, 10-daily – version 3" (DOI: "https://doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465") product of CLMS.

# Launch_me_to_filter
Version 20260212a (Last modified by @JuananMunoz)
Run Launch_me_to_filter.py to filter all the products pending to filter of the "Normalised Difference Vegetation Index 2014-present (raster 300 m), global, 10-daily – version 3" (DOI: "https://doi.org/10.2909/905223f4-2c3d-4cb6-ad8c-d6d065707465") product of CLMS, excluding all pixels with:
  - An uncertainty greater than desired
  - A number of observations smaller than desired
  - Selected flags (See table 7 in the user manual provided by Copernicus)

EXAMPLES:

This example runs the script will all filters deactivated (i.e. the saved NC file will be equal to the original):
    
    run Launch_me_to_filter.py --Filter_by_uncertainty_off --Filter_by_NOBS_off --Filter_bits

These three examples do exactly the same, as they correspond with the default input values:
    
    run Launch_me_to_filter.py
    run Launch_me_to_filter.py --Thr_uncertainty 0.15 --Thr_NOBS 2 --Filter_bits 0 1 2 3 4 5 6 7
    run Launch_me_to_filter.py --Filter_by_uncertainty_on --Thr_uncertainty 0.15 --Filter_by_NOBS_on --Thr_NOBS 2 --Filter_bits 0 1 2 3 4 5 6 7

This example sets the threshold for uncertainty to 0.28 (i.e. all pixels with uncertainties equal or greater than 0.28 will be excluded), deactivates the filter for the Number of Observations and only excludes those pixels with flags in the bits 0, 2, 4 and 7:
    
    run Launch_me_to_filter.py --Thr_uncertainty 0.28 --Filter_by_NOBS_off --Filter_bits 0 2 4 7
    
# WARNINGS ⚠️
The directory must contain a ".credentials.ini" file with your credentials to log in into CDSE (https://dataspace.copernicus.eu/).
This file must follow the next structure:
        
      [cdse]
      username = username@example.org
      password = MyPa5sWoRd

CDSE sets quotas and limitations for the downloads ("https://documentation.dataspace.copernicus.eu/Quotas.html"). Reaching these quotas and limitations triggers a Runtime Error while creating temporary S3 credentials (error #403). 
