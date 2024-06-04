# This directory includes the code and files used to process the raw, binary SASSIE-ECCO model granules to NetCDF and ZARR formats.

Additional code is included that 1) extracts just the surface layer (k=0) of the 3D model fields, and 2) transfers processed files from one private s3 bucket to another.

## /netcdf

## /zarr

## /extract_surface_layer

## /tx_to_podaac

`tx_to_podaac.sh`: Code used to transfer data directories from one private s3 bucket to another private s3 bucket on an EC2.

`summarize_s3_contents.sh`: Script run to summarize the total size (GB) and number of files for each subdirectory within a parent s3 bucket.

Note: These scripts are configured to use private access keys stored in a local credentials file.
