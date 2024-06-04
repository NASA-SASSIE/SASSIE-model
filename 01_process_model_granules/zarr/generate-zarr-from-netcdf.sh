#!/bin/bash

# this script converts one or more netCDF datasets to zarr and pushes them to the cloud.
# The python scipt downloads a SASSIE-ECCO netcdf dataset from a s3 bucket, then creates a zarr store, and finally pushes it to the cloud.

# You will need to specify the nvme disk and whether you want all the files to be processes (--files_to_process -1) or only a subset (--files_to_process $sn $en). Edit these fields below as needed.
# $sn (start number) and $en (end number) are the indices for which files to process.

# there are ~2600 files in a full dataset

declare -a arr=("SALT")

## now loop through the above array
for x in "${arr[@]}"
do
  # sn=0
  # en=200
  disk=1

  # This version processes a subset of the data:
  # cmd="python create-zarr-from-netcdf.py --var_name $x --files_to_process $sn $en --ec2_scratch_dir /nvme_data${disk} --nc_s3_bucket s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF --zarr_s3_bucket s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/ZARR --keep_local_nc_files --keep_local_zarr_store 1> ${x}.log 2> ${x}.err.log &"

  # This version processes all of the netCDf files:
  cmd="python create-zarr-from-netcdf.py --var_name $x --files_to_process -1 --ec2_scratch_dir /nvme_data${disk} --nc_s3_bucket s3://ecco-processed-data/SASSIE/N1/HH/NETCDF/ --zarr_s3_bucket s3://ecco-processed-data/SASSIE/N1/HH/ZARR/ --keep_local_nc_files --keep_local_zarr_store --push_to_s3 1> ${x}.log 2> ${x}.err.log &"
  
  echo $cmd
  eval $cmd
  
  wait
done
